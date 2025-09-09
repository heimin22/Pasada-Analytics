import { Client } from 'pg';
import fetch from 'node-fetch';
import dotenv from 'dotenv';

dotenv.config();

export interface QuestDBConfig {
    pgConnectionString: string;
    ilpEndpoint: string;
    httpEndpoint: string;
    maxConnections?: number;
    connectionTimeout?: number;   
}

export interface WeeklyRouteData {
    week_start: string;
    route_id: number;
    sample_count: number;
    avg_traffic_density: number;
    min_traffic_density: number;
    max_traffic_density: number;
    avg_duration_seconds: number;
    avg_duration_in_traffic_seconds: number;
    avg_traffic_penalty_fraction: number;
    total_distance_meters: number;
    avg_speed_kmh: number;
    peak_hour: number;
}

export interface TrafficData {
    timestamp: string;
    routeId: number;
    trafficDensity: number;
    duration: number;
    durationInTraffic: number;
    distance: number;
    status: string;
}

export interface QuestDBResponse {
    query: string;
    columns: Array<{
        name: string;
        type: string;
    }>;
    dataset: string[][];
    count: number;
}

export class QuestDBService {
    private config: QuestDBConfig;
    private pgClient: Client | null = null;

    constructor(config: QuestDBConfig) {
        this.config = config;
        this.validateConfig();
    }

    private validateConfig() {
        if (!this.config.pgConnectionString) {
            throw new Error('PostgreSQL connection string is required');
        }
        if (!this.config.ilpEndpoint) {
            throw new Error('QuestDB ILP endpoint is required');
        }
        if (!this.config.httpEndpoint) {
            throw new Error('QuestDB HTTP endpoint is required');
        }
    }
    
    async connect(): Promise<void> {
        if (this.pgClient) {
            return;
        }
        this.pgClient = new Client({
            connectionString: this.config.pgConnectionString,
            connectionTimeoutMillis: this.config.connectionTimeout || 30000,
        });

        try {
            await this.pgClient.connect();
            console.log('Connected to Supabase PostgreSQL');
        } catch (error) {
            this.pgClient = null;
            throw new Error(`Failed to connect to PostgreSQL: ${error}`);
        } 
    }

    async disconnect(): Promise<void> {
        if (this.pgClient) {
            await this.pgClient.end();
            this.pgClient = null;
            console.log('Disconnected from Supabase PostgreSQL');
        }
    }

    async getWeeklyAnalytics(weekOffset: number = 1): Promise<WeeklyRouteData[]> {
        if (!this.pgClient) {
          throw new Error('Not connected to database');
        }
    
        // Parameterized query - prevents SQL injection
        const query = `
          WITH week_bounds AS (
            SELECT
              (date_trunc('week', now() AT TIME ZONE 'Asia/Manila') AT TIME ZONE 'Asia/Manila')::date - INTERVAL '%s days' AS start_dt
          )
          SELECT
            (wb.start_dt)::date AS week_start,
            t.route_id,
            COUNT(*) AS sample_count,
            AVG(traffic_density)::numeric(6,4) AS avg_traffic_density,
            MIN(traffic_density)::numeric(6,4) AS min_traffic_density,
            MAX(traffic_density)::numeric(6,4) AS max_traffic_density,
            AVG(duration) FILTER (WHERE duration IS NOT NULL)::numeric(10,2) AS avg_duration_seconds,
            AVG(duration_in_traffic) FILTER (WHERE duration_in_traffic IS NOT NULL)::numeric(10,2) AS avg_duration_in_traffic_seconds,
            AVG(
              CASE WHEN duration IS NOT NULL AND duration > 0 AND duration_in_traffic IS NOT NULL 
                   THEN (duration_in_traffic - duration)::double precision / duration
                   ELSE NULL END
            )::numeric(6,4) AS avg_traffic_penalty_fraction,
            SUM(distance) FILTER (WHERE distance IS NOT NULL)::bigint AS total_distance_meters,
            (CASE 
               WHEN SUM(CASE WHEN duration > 0 THEN distance ELSE 0 END) = 0 THEN NULL
               ELSE (SUM(distance) FILTER (WHERE duration > 0) / NULLIF(SUM(duration) FILTER (WHERE duration > 0),0)) * 3.6
             END)::numeric(10,2) AS avg_speed_kmh,
            (SELECT (array_agg(hour ORDER BY cnt DESC))[1]
             FROM (
               SELECT date_part('hour', timestamp AT TIME ZONE 'Asia/Manila')::int AS hour, COUNT(*) AS cnt
               FROM public.traffic_analytics ta2
               WHERE ta2.route_id = t.route_id
                 AND ta2.timestamp >= wb.start_dt
                 AND ta2.timestamp < wb.start_dt + INTERVAL '7 days'
               GROUP BY hour
             ) h
            )::int AS peak_hour
          FROM public.traffic_analytics t
          CROSS JOIN week_bounds wb
          WHERE t.timestamp >= wb.start_dt
            AND t.timestamp < wb.start_dt + INTERVAL '7 days'
          GROUP BY week_start, t.route_id
          ORDER BY week_start, t.route_id
        `;
    
        try {
          // Use parameterized query to prevent injection
          const weekDays = weekOffset * 7;
          const result = await this.pgClient.query(query.replace('%s', weekDays.toString()));
          return result.rows;
        } catch (error) {
          throw new Error(`Failed to fetch weekly analytics: ${error}`);
        }
    }

    // input validation and sanitization for ILP
    async pushToQuestDB(data: WeeklyRouteData[]): Promise<void> {
        if (!data || data.length === 0) {
            console.log('No data to push to QuestDB');
            return;
        }

        try {
            const lines = data.map(row => this.createILPLine(row)).join('\n');
            
            const response = await fetch(this.config.ilpEndpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'text/plain; charset=utf-8',
                    'User-Agent': 'Pasada-Analytics/1.0',
                },
                body: lines,
            });
      
            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`QuestDB ILP failed: ${response.status} - ${errorText}`);
            }
                console.log(`Successfully pushed ${data.length} rows to QuestDB`);
            } catch (error) {
                throw new Error(`Failed to push to QuestDB: ${error}`);
        }
    }

    private createILPLine(data: WeeklyRouteData): string {
        // input validation
        if (!data.route_id || !data.week_start) {
            throw new Error('Invalid data: route_id and week_start are required');
        }

        // sanitize route_id
        const routeId = Math.abs(parseInt(data.route_id.toString()));
        if (isNaN(routeId)) {
            throw new Error('Invalid route_id: must be numeric');
        }

        // create timestamp - use Monday midnight Manila time
        const weekStart = new Date(`${data.week_start}T00:00:00+08:00`);
        if (isNaN(weekStart.getTime())) {
            throw new Error('Invalid week_start: must be a valid date');
        }

        const tsNanos = (BigInt(weekStart.getTime()) * BigInt(1000000)).toString();
        
        // tags - only for route_id as tag for efficient querying
        const tags = `route_id=${routeId}`;

        // build fields - validate and sanitize numeric values
        const fields: string[] = [];

        // required fields
        fields.push(`sample_count=${this.sanitizeInteger(data.sample_count)}i`);
        
        // Optional fields with null checks and validation
        if (this.isValidNumber(data.avg_traffic_density)) {
            fields.push(`avg_traffic_density=${this.sanitizeFloat(data.avg_traffic_density, 4)}`);
        }
        if (this.isValidNumber(data.min_traffic_density)) {
            fields.push(`min_traffic_density=${this.sanitizeFloat(data.min_traffic_density, 4)}`);
        }
        if (this.isValidNumber(data.max_traffic_density)) {
            fields.push(`max_traffic_density=${this.sanitizeFloat(data.max_traffic_density, 4)}`);
        }
        if (this.isValidNumber(data.avg_duration_seconds)) {
            fields.push(`avg_duration_seconds=${this.sanitizeInteger(data.avg_duration_seconds)}i`);
        }
        if (this.isValidNumber(data.avg_duration_in_traffic_seconds)) {
            fields.push(`avg_duration_in_traffic_seconds=${this.sanitizeInteger(data.avg_duration_in_traffic_seconds)}i`);
        }
        if (this.isValidNumber(data.avg_traffic_penalty_fraction)) {
            fields.push(`avg_traffic_penalty_fraction=${this.sanitizeFloat(data.avg_traffic_penalty_fraction, 4)}`);
        }
        if (this.isValidNumber(data.total_distance_meters)) {
            fields.push(`total_distance_meters=${this.sanitizeInteger(data.total_distance_meters)}i`);
        }
        if (this.isValidNumber(data.avg_speed_kmh)) {
            fields.push(`avg_speed_kmh=${this.sanitizeFloat(data.avg_speed_kmh, 2)}`);
        }
        if (this.isValidNumber(data.peak_hour)) {
            fields.push(`peak_hour=${this.sanitizeInteger(data.peak_hour)}i`);
        }

        if (fields.length === 0) {
            throw new Error('No valid fields found in data');
        }

        return `route_weekly_summary,${tags} ${fields.join(',')} ${tsNanos}`;
    }

    private isValidNumber(value: number): boolean {
        return value !== null && value !== undefined && !isNaN(Number(value));
    }

    private sanitizeInteger(value: number): number {
        const num = parseInt(value.toString());
        if (isNaN(num)) {
            throw new Error(`Invalid value: ${value}`);
        }
        return num;
    }

    private sanitizeFloat(value: number, precision: number = 2): number {
        const num = parseFloat(value.toString());
        if (isNaN(num)) {
            throw new Error(`Invalid float: ${value}`);
        }
        return Math.round(num * Math.pow(10, precision)) / Math.pow(10, precision);
    }

    async saveTrafficDataToQuestDB(trafficData: TrafficData[]): Promise<void> {
        if (!trafficData || trafficData.length === 0) return;
    
        try {
          const lines = trafficData.map(data => {
            const tsNanos = (BigInt(new Date(data.timestamp).getTime()) * BigInt(1_000_000)).toString();
            const tags = `route_id=${this.sanitizeInteger(data.routeId)}`;
            
            const fields = [
              `traffic_density=${this.sanitizeFloat(data.trafficDensity, 4)}`,
              `duration=${this.sanitizeInteger(data.duration)}i`,
              `duration_in_traffic=${this.sanitizeInteger(data.durationInTraffic)}i`,
              `distance=${this.sanitizeInteger(data.distance)}i`,
              `status="${data.status}"`
            ];
    
            return `traffic_analytics,${tags} ${fields.join(',')} ${tsNanos}`;
          }).join('\n');
    
          await fetch(this.config.ilpEndpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'text/plain; charset=utf-8' },
            body: lines,
          });
    
          console.log(`✅ Saved ${trafficData.length} traffic data points to QuestDB`);
        } catch (error) {
          console.error('Failed to save traffic data to QuestDB:', error);
          throw error;
        }
      }
    
      // Query QuestDB for analytics
      async queryAnalytics(query: string, params: Record<string, string> = {}): Promise<string[][]> {
        try {
          // Build URL with query parameters
          const url = new URL(this.config.httpEndpoint + '/exec');
          url.searchParams.set('query', query);
          
          // Add additional parameters if provided
          Object.entries(params).forEach(([key, value]) => {
            url.searchParams.set(key, value);
          });
          
          const response = await fetch(url.toString(), {
            method: 'GET',
            headers: { 
              'Accept': 'application/json',
              'User-Agent': 'Pasada-Analytics/1.0'
            }
          });
    
          if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`QuestDB query failed: ${response.status} - ${errorText}`);
          }
    
          const result = await response.json() as QuestDBResponse;
          return result.dataset || [];
        } catch (error) {
          throw new Error(`Analytics query failed: ${error}`);
        }
    }
}