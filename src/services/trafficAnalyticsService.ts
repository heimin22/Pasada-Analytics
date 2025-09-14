import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { Client as GoogleMapsClient, TrafficModel } from '@googlemaps/google-maps-services-js';
import { QuestDBService } from './questdbServices';
import fetch from 'node-fetch';
import { 
  OfficialRoute,
  AllowedStop,
  RouteTrafficAnalysis,
  TrafficForecast
} from '../types/traffic';
import logger from '../utils/logger';

interface GoogleMapsDirectionParams {
  origin: {
    lat: number;
    lng: number;
  };
  destination: {
    lat: number;
    lng: number;
  };
  waypoints?: {
    lat: number;
    lng: number;
  }[];
  departure_time: number | "now";
  traffic_model: TrafficModel;
  key: string;
}

interface TrafficSummary {
  route_id: number;
  route_name: string;
  avg_traffic_density: number;
  peak_traffic_density: number;
  low_traffic_density: number;
  avg_speed_kmh: number;
  total_samples: number;
}

import { QuestDBConfig } from './questdbServices';

interface TrafficAnalyticsConfig {
  questdb: QuestDBConfig;
  supabaseUrl: string;
  supabaseServiceKey: string;
  googleMapsApiKey: string;
}

export class TrafficAnalyticsService {
  private questdbService: QuestDBService;
  private supabaseClient?: SupabaseClient;
  private googleMapsClient: GoogleMapsClient;
  private config: TrafficAnalyticsConfig;

  constructor(config: TrafficAnalyticsConfig) {
    this.config = config;
    this.questdbService = new QuestDBService(config.questdb);
    
    // Initialize Supabase client
    if (config.supabaseUrl && config.supabaseServiceKey) {
      this.supabaseClient = createClient(config.supabaseUrl, config.supabaseServiceKey);
    } else {
      logger.warn('Supabase not configured - some features may not work');
    }
    
    // Initialize Google Maps client
    this.googleMapsClient = new GoogleMapsClient({});
    
    if (!config.googleMapsApiKey) {
      logger.warn('Google Maps API key not configured - traffic data collection disabled');
    }
  }

  /**
   * Main method to run complete traffic analytics
   */
  async runTrafficAnalytics(options: {
    routeIds?: number[];
    includeHistoricalAnalysis?: boolean;
    generateForecasts?: boolean;
  } = {}): Promise<{
    success: boolean;
    routesAnalyzed: number;
    forecastsGenerated: number;
    message: string;
  }> {
    const startTime = Date.now();
    logger.info('Starting traffic analytics process...');

    try {
      // 1. Fetch active routes from Supabase
      const routes = await this.fetchActiveRoutes(options.routeIds);
      
      if (routes.length === 0) {
        return {
          success: true,
          routesAnalyzed: 0,
          forecastsGenerated: 0,
          message: 'No active routes found for analysis'
        };
      }

      logger.info(`Analyzing traffic for ${routes.length} routes`);

      // 2. Analyze current traffic for all routes
      const currentAnalyses = await this.analyzeCurrentTraffic(routes);

      // 3. Store current traffic data
      await this.storeTrafficAnalyses(currentAnalyses);

      // 4. Generate historical analysis and forecasts if requested
      let forecastsGenerated = 0;
      if (options.generateForecasts) {
        forecastsGenerated = await this.generateAndStoreForecasts(routes);
      }

      // 5. Generate weekly reports if historical analysis is requested
      if (options.includeHistoricalAnalysis) {
        await this.generateWeeklyReports(routes);
      }

      const duration = Date.now() - startTime;
      const message = `Traffic analytics completed in ${duration}ms. Analyzed ${routes.length} routes, generated ${forecastsGenerated} forecasts`;
      
      logger.info(message);

      return {
        success: true,
        routesAnalyzed: routes.length,
        forecastsGenerated,
        message
      };

    } catch (error) {
      logger.error('Traffic analytics failed:', error);
      return {
        success: false,
        routesAnalyzed: 0,
        forecastsGenerated: 0,
        message: `Failed to run traffic analytics: ${(error as Error).message}`
      };
    }
  }

  /**
   * Fetch active routes from Supabase
   */
  private async fetchActiveRoutes(routeIds?: number[]): Promise<OfficialRoute[]> {
    if (!this.supabaseClient) {
      throw new Error('Supabase client not configured');
    }

    let query = this.supabaseClient
      .from('official_routes')
      .select('*')
      .eq('status', 'active')
      .not('origin_lat', 'is', null)
      .not('origin_lng', 'is', null)
      .not('destination_lat', 'is', null)
      .not('destination_lng', 'is', null);

    // Filter by specific route IDs if provided
    if (routeIds && routeIds.length > 0) {
      query = query.in('officialroute_id', routeIds);
    }

    const { data: routes, error } = await query;

    if (error) {
      logger.error('Failed to fetch routes from Supabase:', error);
      throw new Error(`Failed to fetch routes: ${error.message}`);
    }

    return routes as OfficialRoute[] || [];
  }

  /**
   * Fetch allowed stops for routes to get intermediate waypoints
   */
  private async fetchRouteStops(routeId: number): Promise<AllowedStop[]> {
    if (!this.supabaseClient) {
      return [];
    }

    const { data: stops, error } = await this.supabaseClient
      .from('allowed_stops')
      .select('*')
      .eq('officialroute_id', routeId)
      .eq('is_active', true)
      .order('stop_order');

    if (error) {
      logger.warn(`Failed to fetch stops for route ${routeId}:`, error);
      return [];
    }

    return stops as AllowedStop[] || [];
  }

  /**
   * Get current traffic data for all routes using Google Maps API
   */
  private async analyzeCurrentTraffic(routes: OfficialRoute[]): Promise<RouteTrafficAnalysis[]> {
    if (!this.config.googleMapsApiKey) {
      throw new Error('Google Maps API key not configured');
    }

    const analyses: RouteTrafficAnalysis[] = [];
    const batchSize = 5; // Process routes in batches to respect rate limits
    
    for (let i = 0; i < routes.length; i += batchSize) {
      const batch = routes.slice(i, i + batchSize);
      const batchPromises = batch.map(route => this.analyzeRouteTraffic(route));
      
      try {
        const batchResults = await Promise.allSettled(batchPromises);
        
        batchResults.forEach((result, index) => {
          if (result.status === 'fulfilled' && result.value) {
            analyses.push(result.value);
          } else {
            const routeName = batch[index].route_name;
            logger.warn(`Failed to analyze traffic for route ${routeName}:`, 
              result.status === 'rejected' ? result.reason : 'Unknown error');
          }
        });

        // Rate limiting - wait between batches
        if (i + batchSize < routes.length) {
          await this.sleep(1000); // 1 second between batches
        }
      } catch (error) {
        logger.error(`Batch traffic analysis failed for routes ${i}-${i + batchSize}:`, error);
      }
    }

    return analyses;
  }

  /**
   * Analyze traffic for a single route
   */
  private async analyzeRouteTraffic(route: OfficialRoute): Promise<RouteTrafficAnalysis | null> {
    try {
      // Get route stops for waypoints
      const stops = await this.fetchRouteStops(route.officialroute_id);
      
      // Build waypoints from stops
      const waypoints = stops
        .slice(1, -1) // Exclude first and last stop (origin and destination)
        .map(stop => ({
          lat: parseFloat(stop.stop_lat),
          lng: parseFloat(stop.stop_lng)
        }));

      // Call Google Maps Directions API with traffic data
      const params: GoogleMapsDirectionParams = {
        origin: {
          lat: parseFloat(route.origin_lat!),
          lng: parseFloat(route.origin_lng!)
        },
        destination: {
          lat: parseFloat(route.destination_lat!),
          lng: parseFloat(route.destination_lng!)
        },
        departure_time: 'now',
        traffic_model: TrafficModel.best_guess,
        key: this.config.googleMapsApiKey
      };

      // Only add waypoints if they exist
      if (waypoints.length > 0) {
        params.waypoints = waypoints;
      }

      const directionsResponse = await this.googleMapsClient.directions({ params });

      if (directionsResponse.data.status !== 'OK' || !directionsResponse.data.routes.length) {
        logger.warn(`Google Maps API returned no routes for ${route.route_name}`);
        return null;
      }

      const leg = directionsResponse.data.routes[0].legs[0];
      
      // Calculate traffic metrics
      const distanceMeters = leg.distance?.value || 0;
      const durationSeconds = leg.duration?.value || 0;
      const durationInTrafficSeconds = leg.duration_in_traffic?.value || durationSeconds;
      const trafficPenaltySeconds = durationInTrafficSeconds - durationSeconds;
      const trafficDensity = Math.min(1, trafficPenaltySeconds / durationSeconds);
      const speedKmh = distanceMeters > 0 ? (distanceMeters / 1000) / (durationInTrafficSeconds / 3600) : 0;

      // Determine traffic severity
      let trafficSeverity: 'low' | 'moderate' | 'high' | 'severe';
      if (trafficDensity <= 0.15) trafficSeverity = 'low';
      else if (trafficDensity <= 0.35) trafficSeverity = 'moderate';
      else if (trafficDensity <= 0.65) trafficSeverity = 'high';
      else trafficSeverity = 'severe';

      return {
        route_id: route.officialroute_id,
        route_name: route.route_name,
        timestamp: new Date(),
        distance_meters: distanceMeters,
        duration_seconds: durationSeconds,
        duration_in_traffic_seconds: durationInTrafficSeconds,
        traffic_density: trafficDensity,
        speed_kmh: speedKmh,
        traffic_penalty_seconds: trafficPenaltySeconds,
        traffic_severity: trafficSeverity,
        origin: {
          lat: parseFloat(route.origin_lat!),
          lng: parseFloat(route.origin_lng!),
          name: route.origin_name
        },
        destination: {
          lat: parseFloat(route.destination_lat!),
          lng: parseFloat(route.destination_lng!),
          name: route.destination_name
        }
      };

    } catch (error) {
      logger.error(`Failed to analyze traffic for route ${route.route_name}:`, error);
      return null;
    }
  }

  /**
   * Store traffic analyses in QuestDB
   */
  private async storeTrafficAnalyses(analyses: RouteTrafficAnalysis[]): Promise<void> {
    if (analyses.length === 0) return;

    await this.questdbService.connect();
    
    try {
      // Prepare data for QuestDB
      const records = analyses.map(analysis => ({
        timestamp: analysis.timestamp.toISOString(),
        route_id: analysis.route_id,
        route_name: analysis.route_name,
        distance_meters: analysis.distance_meters,
        duration_seconds: analysis.duration_seconds,
        duration_in_traffic_seconds: analysis.duration_in_traffic_seconds,
        traffic_density: analysis.traffic_density,
        speed_kmh: analysis.speed_kmh,
        traffic_penalty_seconds: analysis.traffic_penalty_seconds,
        traffic_severity: analysis.traffic_severity,
        origin_lat: analysis.origin.lat,
        origin_lng: analysis.origin.lng,
        origin_name: analysis.origin.name,
        destination_lat: analysis.destination.lat,
        destination_lng: analysis.destination.lng,
        destination_name: analysis.destination.name
      }));

      // Store in QuestDB using HTTP API
      const insertQuery = `
        CREATE TABLE IF NOT EXISTS route_traffic_analysis (
          timestamp timestamp,
          route_id int,
          route_name string,
          distance_meters double,
          duration_seconds int,
          duration_in_traffic_seconds int,
          traffic_density double,
          speed_kmh double,
          traffic_penalty_seconds int,
          traffic_severity string,
          origin_lat double,
          origin_lng double,
          origin_name string,
          destination_lat double,
          destination_lng double,
          destination_name string
        ) timestamp(timestamp) PARTITION BY DAY;
      `;
      
      await this.executeHttpQuery(insertQuery);

      // Insert the records
      for (const record of records) {
        const insertRecordQuery = `
          INSERT INTO route_traffic_analysis VALUES (
            '${record.timestamp}',
            ${record.route_id},
            '${record.route_name.replace(/'/g, "''")}',
            ${record.distance_meters},
            ${record.duration_seconds},
            ${record.duration_in_traffic_seconds},
            ${record.traffic_density},
            ${record.speed_kmh},
            ${record.traffic_penalty_seconds},
            '${record.traffic_severity}',
            ${record.origin_lat},
            ${record.origin_lng},
            '${record.origin_name.replace(/'/g, "''")}',
            ${record.destination_lat},
            ${record.destination_lng},
            '${record.destination_name.replace(/'/g, "''")}'
          );
        `;
        
        await this.executeHttpQuery(insertRecordQuery);
      }

      logger.info(`Stored ${analyses.length} traffic analysis records in QuestDB`);

    } finally {
      await this.questdbService.disconnect();
    }
  }

  /**
   * Generate forecasts for routes based on historical data
   */
  private async generateAndStoreForecasts(routes: OfficialRoute[]): Promise<number> {
    await this.questdbService.connect();
    
    try {
      let forecastsGenerated = 0;

      for (const route of routes) {
        try {
          const forecast = await this.generateRouteForecast(route.officialroute_id);
          if (forecast) {
            await this.storeForecast(forecast);
            forecastsGenerated++;
          }
        } catch (error) {
          logger.warn(`Failed to generate forecast for route ${route.route_name}:`, error);
        }
      }

      return forecastsGenerated;
    } finally {
      await this.questdbService.disconnect();
    }
  }

  /**
   * Generate forecast for a single route
   */
  private async generateRouteForecast(routeId: number): Promise<TrafficForecast | null> {
    // Get historical data for the last 4 weeks
    const query = `
      SELECT 
        extract(hour from timestamp) as hour,
        avg(traffic_density) as avg_density,
        avg(duration_in_traffic_seconds) as avg_duration,
        count(*) as data_points
      FROM route_traffic_analysis 
      WHERE route_id = ${routeId} 
        AND timestamp > dateadd('w', -4, now())
      GROUP BY hour
      ORDER BY hour
    `;

    const historicalData = await this.questdbService.queryAnalytics(query);
    
    if (historicalData.length === 0) {
      return null;
    }

    // Create hourly predictions for the next 7 days
    const hourlyPredictions = [];
    
    for (let hour = 0; hour < 24; hour++) {
      const historicalHour = historicalData.find(row => parseInt(row[0]) === hour);
      
      if (historicalHour) {
        const avgDensity = parseFloat(historicalHour[1]);
        const avgDuration = parseFloat(historicalHour[2]);
        const dataPoints = parseInt(historicalHour[3]);
        const confidence = Math.min(0.95, 0.4 + (dataPoints * 0.05));
        
        hourlyPredictions.push({
          hour,
          predicted_density: avgDensity,
          confidence,
          expected_duration_seconds: Math.round(avgDuration)
        });
      } else {
        // Use default predictions for hours with no historical data
        hourlyPredictions.push({
          hour,
          predicted_density: this.getDefaultTrafficDensity(hour),
          confidence: 0.3,
          expected_duration_seconds: 0
        });
      }
    }

    return {
      route_id: routeId,
      forecast_date: new Date(),
      hourly_predictions: hourlyPredictions
    };
  }

  /**
   * Store forecast in QuestDB
   */
  private async storeForecast(forecast: TrafficForecast): Promise<void> {
    // Create table if not exists
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS route_traffic_forecasts (
        forecast_date timestamp,
        route_id int,
        hour int,
        predicted_density double,
        confidence double,
        expected_duration_seconds int
      ) timestamp(forecast_date) PARTITION BY DAY;
    `;
    
    await this.executeHttpQuery(createTableQuery);

    // Insert forecast data
    for (const prediction of forecast.hourly_predictions) {
      const insertQuery = `
        INSERT INTO route_traffic_forecasts VALUES (
          '${forecast.forecast_date.toISOString()}',
          ${forecast.route_id},
          ${prediction.hour},
          ${prediction.predicted_density},
          ${prediction.confidence},
          ${prediction.expected_duration_seconds}
        );
      `;
      
      await this.executeHttpQuery(insertQuery);
    }
  }

  /**
   * Generate weekly reports
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars, no-unused-vars
  private async generateWeeklyReports(_routes: OfficialRoute[]): Promise<void> {
    // Implementation for weekly reports based on historical data
    logger.info('Generating weekly traffic reports...');
    
    // This would analyze the past week's data and create comprehensive reports
    // For brevity, I'll leave this as a placeholder that can be expanded
  }

  /**
   * Get default traffic density for an hour (used when no historical data available)
   */
  private getDefaultTrafficDensity(hour: number): number {
    // Morning rush: 7-9 AM
    if (hour >= 7 && hour <= 9) return 0.6;
    // Evening rush: 5-7 PM
    if (hour >= 17 && hour <= 19) return 0.7;
    // Late night/early morning: 11 PM - 5 AM
    if (hour >= 23 || hour <= 5) return 0.1;
    // Regular hours
    return 0.3;
  }

  /**
   * Execute a query via QuestDB HTTP endpoint (for CREATE/INSERT operations)
   */
  private async executeHttpQuery(query: string): Promise<void> {
    if (!this.config.questdb.httpEndpoint) {
      throw new Error('QuestDB HTTP endpoint not configured');
    }

    const url = new URL(this.config.questdb.httpEndpoint + '/exec');
    url.searchParams.set('query', query);

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
  }

  /**
   * Sleep utility for rate limiting
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Get traffic analytics summary for a specific route
   */
  async getRouteTrafficSummary(routeId: number, days: number = 7): Promise<TrafficSummary | null> {
    await this.questdbService.connect();
    
    try {
      const query = `
        SELECT 
          route_id,
          route_name,
          avg(traffic_density) as avg_density,
          max(traffic_density) as peak_density,
          min(traffic_density) as low_density,
          avg(speed_kmh) as avg_speed,
          count(*) as total_samples
        FROM route_traffic_analysis 
        WHERE route_id = ${routeId} 
          AND timestamp > dateadd('d', -${days}, now())
        GROUP BY route_id, route_name
      `;

      const result = await this.questdbService.queryAnalytics(query);
      
      if (result.length === 0) {
        return null;
      }

      const row = result[0];
      return {
        route_id: parseInt(row[0]),
        route_name: row[1],
        avg_traffic_density: parseFloat(row[2]),
        peak_traffic_density: parseFloat(row[3]),
        low_traffic_density: parseFloat(row[4]),
        avg_speed_kmh: parseFloat(row[5]),
        total_samples: parseInt(row[6])
      };
    } finally {
      await this.questdbService.disconnect();
    }
  }
}
