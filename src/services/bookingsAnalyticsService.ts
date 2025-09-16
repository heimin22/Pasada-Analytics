import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { env } from '../config/environment';
import fetch from 'node-fetch';
import logger from '../utils/logger';

export interface BookingFrequencyPoint {
  date: string; // YYYY-MM-DD
  count: number;
}

export interface BookingFrequencyForecastPoint {
  date: string; // YYYY-MM-DD
  predicted_count: number;
  confidence: number;
}

export interface BookingFrequencyResult {
  history: BookingFrequencyPoint[];
  forecast: BookingFrequencyForecastPoint[];
}

export class BookingsAnalyticsService {
  private supabaseClient: SupabaseClient;

  constructor() {
    if (!env.supabaseUrl || !env.supabaseServiceRoleKey) {
      throw new Error('Supabase not configured');
    }
    this.supabaseClient = createClient(env.supabaseUrl, env.supabaseServiceRoleKey);
  }

  // Fetch daily booking counts for the past N days (default 90)
  async getDailyBookingCounts(days: number = 90): Promise<BookingFrequencyPoint[]> {
    const since = new Date();
    since.setDate(since.getDate() - days);

    // Use RPC or SQL via PostgREST? Supabase JS supports REST filters; we'll fetch created_at >= since and group client-side.
    const { data, error } = await this.supabaseClient
      .from('bookings')
      .select('created_at', { count: 'exact', head: false })
      .gte('created_at', since.toISOString())
      .order('created_at', { ascending: true });

    if (error) {
      logger.error('Failed to fetch bookings from Supabase', error);
      throw new Error(`Failed to fetch bookings: ${error.message}`);
    }

    const buckets = new Map<string, number>();

    (data || []).forEach((row: { created_at: string }) => {
      const d = new Date(row.created_at);
      const key = d.toISOString().slice(0, 10); // YYYY-MM-DD
      buckets.set(key, (buckets.get(key) || 0) + 1);
    });

    // Fill missing days with zero for stability
    const result: BookingFrequencyPoint[] = [];
    const cursor = new Date(since);
    const today = new Date();
    while (cursor <= today) {
      const key = cursor.toISOString().slice(0, 10);
      result.push({ date: key, count: buckets.get(key) || 0 });
      cursor.setDate(cursor.getDate() + 1);
    }

    return result;
  }

  // Naive 7-day forecast using day-of-week seasonal average
  async getSevenDayForecast(daysHistory: number = 90): Promise<BookingFrequencyResult> {
    const history = await this.getDailyBookingCounts(daysHistory);

    // Compute average per day-of-week
    const sums = Array.from({ length: 7 }, () => ({ sum: 0, n: 0 }));
    history.forEach((p) => {
      const dow = new Date(p.date + 'T00:00:00Z').getUTCDay();
      sums[dow].sum += p.count;
      sums[dow].n += 1;
    });

    const avgs = sums.map((s) => (s.n > 0 ? s.sum / s.n : 0));

    // Simple trend: last 14 days linear slope
    const lastN = history.slice(-14);
    let slope = 0;
    if (lastN.length >= 2) {
      const n = lastN.length;
      const xMean = (n - 1) / 2;
      const yMean = lastN.reduce((a, p) => a + p.count, 0) / n;
      let num = 0;
      let den = 0;
      lastN.forEach((p, i) => {
        num += (i - xMean) * (p.count - yMean);
        den += (i - xMean) * (i - xMean);
      });
      slope = den > 0 ? num / den : 0;
    }

    const forecast: BookingFrequencyForecastPoint[] = [];
    const start = new Date();
    for (let i = 1; i <= 7; i++) {
      const d = new Date(start);
      d.setDate(start.getDate() + i);
      const dow = d.getUTCDay();
      const base = avgs[dow];
      const trendAdj = slope * (history.length + i - history.length); // roughly slope per day
      const predicted = Math.max(0, Math.round(base + trendAdj));
      const confidence = Math.min(0.95, 0.4 + Math.min(1, (sums[dow].n / 8)) * 0.5);
      forecast.push({ date: d.toISOString().slice(0, 10), predicted_count: predicted, confidence });
    }

    return { history, forecast };
  }

  // Ensure QuestDB tables exist
  private async ensureQuestDBTables(): Promise<void> {
    if (!env.questdb.httpEndpoint) {
      throw new Error('QuestDB HTTP endpoint not configured');
    }
    const createDailyCounts = `
      CREATE TABLE IF NOT EXISTS booking_daily_counts (
        day timestamp,
        route_id int,
        total_bookings long,
        created_at timestamp
      ) timestamp(day) PARTITION BY DAY;`;

    const createForecasts = `
      CREATE TABLE IF NOT EXISTS booking_forecasts (
        forecast_date timestamp,
        target_day timestamp,
        route_id int,
        predicted_count long,
        confidence double
      ) timestamp(forecast_date) PARTITION BY DAY;`;

    await this.execQuestDB(createDailyCounts);
    await this.execQuestDB(createForecasts);
  }

  // Persist computed daily counts into QuestDB
  async persistDailyCounts(daysHistory: number = 90): Promise<{ rows: number }> {
    await this.ensureQuestDBTables();
    const history = await this.getDailyBookingCounts(daysHistory);
    let rows = 0;
    for (const p of history) {
      const insert = `
        INSERT INTO booking_daily_counts (
          day, route_id, total_bookings, created_at
        ) VALUES (
          '${p.date}T00:00:00.000Z',
          null,
          ${p.count},
          '${new Date().toISOString()}'
        );`;
      await this.execQuestDB(insert);
      rows++;
    }
    return { rows };
  }

  // Persist a 7-day forecast into QuestDB
  async persistForecast(daysHistory: number = 90): Promise<{ rows: number; forecast_date: string }> {
    await this.ensureQuestDBTables();
    const { forecast } = await this.getSevenDayForecast(daysHistory);
    const forecastDateIso = new Date().toISOString();
    let rows = 0;
    for (const f of forecast) {
      const insert = `
        INSERT INTO booking_forecasts (
          forecast_date, target_day, route_id, predicted_count, confidence
        ) VALUES (
          '${forecastDateIso}',
          '${f.date}T00:00:00.000Z',
          null,
          ${f.predicted_count},
          ${f.confidence}
        );`;
      await this.execQuestDB(insert);
      rows++;
    }
    return { rows, forecast_date: forecastDateIso };
  }

  // Read daily counts from QuestDB for the past N days
  async getDailyCountsFromQuestDB(days: number = 90): Promise<BookingFrequencyPoint[]> {
    const query = `
      SELECT day, total_bookings
      FROM booking_daily_counts
      WHERE day > dateadd('d', -${days}, now())
      ORDER BY day ASC;`;
    const rows = await this.queryQuestDB(query);
    return rows.map(r => ({ 
      date: new Date(r[0]).toISOString().slice(0, 10), 
      count: parseInt(r[1]) 
    }));
  }

  // Read the latest forecast set from QuestDB (next 7 target days)
  async getLatestForecastFromQuestDB(): Promise<BookingFrequencyForecastPoint[]> {
    const rows = await this.queryQuestDB(`
      SELECT target_day, predicted_count, confidence
      FROM booking_forecasts
      WHERE forecast_date = (SELECT max(forecast_date) FROM booking_forecasts)
      ORDER BY target_day ASC;`);
    return rows.map(r => ({ 
      date: new Date(r[0]).toISOString().slice(0, 10), 
      predicted_count: parseInt(r[1]), 
      confidence: parseFloat(r[2]) 
    }));
  }

  private async execQuestDB(query: string): Promise<void> {
    const url = new URL(env.questdb.httpEndpoint + '/exec');
    url.searchParams.set('query', query);
    await this.withRetry(async () => {
      const response = await fetch(url.toString(), {
        method: 'GET',
        headers: { 'Accept': 'application/json', 'User-Agent': 'Pasada-Analytics/1.0' }
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`QuestDB exec failed: ${response.status} - ${text}`);
      }
    });
  }

  private async queryQuestDB(query: string): Promise<string[][]> {
    const url = new URL(env.questdb.httpEndpoint + '/exec');
    url.searchParams.set('query', query);
    return await this.withRetry(async () => {
      const response = await fetch(url.toString(), {
        method: 'GET',
        headers: { 'Accept': 'application/json', 'User-Agent': 'Pasada-Analytics/1.0' }
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`QuestDB query failed: ${response.status} - ${text}`);
      }
      const json = await response.json() as { dataset?: string[][] };
      return json.dataset || [];
    });
  }

  private async withRetry<T>(fn: () => Promise<T>, attempts: number = 3, delayMs: number = 250): Promise<T> {
    let lastError: unknown;
    for (let i = 0; i < attempts; i++) {
      try {
        return await fn();
      } catch (err) {
        lastError = err;
        if (i < attempts - 1) {
          await new Promise(res => setTimeout(res, delayMs * (i + 1)));
          continue;
        }
      }
    }
    throw lastError as Error;
  }
}


