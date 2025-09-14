/* eslint-disable no-console */
import { QuestDBService } from './questdbServices';
import { WeeklyAnalyticsService } from './weeklyAnalyticsService';
import { TrafficAnalyticsService } from './trafficAnalyticsService';
import { TrafficData, TrafficPrediction } from '../types/traffic';
import { DatabaseService } from '../types/services';
import { env } from '../config/environment';

interface TrafficAnalyticsResult {
  success: boolean;
  routesAnalyzed: number;
  forecastsGenerated: number;
  message: string;
}

export class AnalyticsService {
  private questdbService?: QuestDBService;
  private weeklyAnalyticsService?: WeeklyAnalyticsService;
  private trafficAnalyticsService?: TrafficAnalyticsService;

  constructor(
    private databaseService: DatabaseService | null
  ) {
    console.log('AnalyticsService constructor called');
    try {
      // Initialize QuestDB services if configured
      console.log('Initializing QuestDB in AnalyticsService...');
      this.initializeQuestDB();
      console.log('Initializing Traffic Analytics in AnalyticsService...');
      this.initializeTrafficAnalytics();
      console.log('AnalyticsService initialization complete');
    } catch (error) {
      console.error('Error in AnalyticsService constructor:', error);
      throw error;
    }
  }

  private initializeQuestDB(): void {
    if (process.env.QUESTDB_ILP && process.env.QUESTDB_HTTP) {
      const config = {
        pgConnectionString: process.env.PG_CONN || '',
        ilpEndpoint: process.env.QUESTDB_ILP,
        httpEndpoint: process.env.QUESTDB_HTTP,
      };
      
      this.questdbService = new QuestDBService(config);
      this.weeklyAnalyticsService = new WeeklyAnalyticsService(config);
    }
  }

  private initializeTrafficAnalytics(): void {
    try {
      console.log('Checking traffic analytics configuration...');
      console.log('supabaseUrl:', !!env.supabaseUrl);
      console.log('supabaseServiceRoleKey:', !!env.supabaseServiceRoleKey);
      console.log('googleMapsApiKey:', !!env.googleMapsApiKey);
      console.log('questdb:', !!env.questdb);
      
      if (env.supabaseUrl && env.supabaseServiceRoleKey && env.googleMapsApiKey && env.questdb) {
        console.log('All traffic analytics config available, creating service...');
        const trafficConfig = {
          questdb: env.questdb,
          supabaseUrl: env.supabaseUrl,
          supabaseServiceKey: env.supabaseServiceRoleKey,
          googleMapsApiKey: env.googleMapsApiKey
        };
        
        this.trafficAnalyticsService = new TrafficAnalyticsService(trafficConfig);
        console.log('TrafficAnalyticsService created successfully');
      } else {
        console.log('Traffic analytics configuration incomplete, skipping service creation');
      }
    } catch (error) {
      console.error('Traffic analytics service initialization failed:', error);
      throw error;
    }
  }

  // Enhanced method to save traffic data to both Supabase and QuestDB
  async saveTrafficDataDual(trafficData: TrafficData[]): Promise<void> {
    try {
      // Save to Supabase (existing functionality)
      if (this.databaseService) {
        await this.databaseService.saveTrafficData(trafficData);
      }
      
      // Also save to QuestDB for real-time analytics
      if (this.questdbService) {
        await this.questdbService.saveTrafficDataToQuestDB(trafficData);
      }
    } catch (error) {
      console.error('Failed to save traffic data:', error);
      throw error;
    }
  }

  // New method for enhanced predictions using QuestDB
  async getEnhancedPredictions(routeId: number): Promise<TrafficPrediction[]> {
    if (this.weeklyAnalyticsService) {
      try {
        return await this.weeklyAnalyticsService.generateTrafficPredictions(routeId);
      } catch (error) {
        console.warn('QuestDB predictions failed, falling back to local method:', error);
      }
    }
    
    // Fallback to basic predictions if QuestDB is not available
    return this.generateBasicPredictions(routeId);
  }

  // Fallback method for basic predictions
  // eslint-disable-next-line @typescript-eslint/no-unused-vars, no-unused-vars
  private generateBasicPredictions(_routeId: number): TrafficPrediction[] {
    const predictions: TrafficPrediction[] = [];
    const now = new Date();

    // Generate basic predictions for the next 7 days
    for (let day = 1; day <= 7; day++) {
      const predictionDate = new Date(now);
      predictionDate.setDate(now.getDate() + day);
      
      [7, 9, 12, 17, 19, 22].forEach(hour => {
        predictions.push({
          date: new Date(predictionDate.getFullYear(), predictionDate.getMonth(), predictionDate.getDate(), hour),
          predictedDensity: this.getBasicDensityForHour(hour, predictionDate.getDay()),
          confidence: 0.3, // Low confidence for basic predictions
          timeOfDay: `${hour.toString().padStart(2, '0')}:00`
        });
      });
    }

    return predictions;
  }

  private getBasicDensityForHour(hour: number, dayOfWeek: number): number {
    const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
    
    if (hour >= 7 && hour <= 9) {
      return isWeekend ? 0.4 : 0.7; // Morning rush
    } else if (hour >= 17 && hour <= 19) {
      return isWeekend ? 0.5 : 0.8; // Evening rush
    } else if (hour >= 22 || hour <= 6) {
      return 0.2; // Late night/early morning
    }
    
    return 0.3; // Default
  }

  /**
   * Run comprehensive traffic analytics for all active routes
   */
  async runTrafficAnalytics(options: {
    routeIds?: number[];
    includeHistoricalAnalysis?: boolean;
    generateForecasts?: boolean;
  } = {}): Promise<TrafficAnalyticsResult> {
    if (!this.trafficAnalyticsService) {
      throw new Error('Traffic analytics service not initialized');
    }

    return await this.trafficAnalyticsService.runTrafficAnalytics(options);
  }

  /**
   * Get traffic summary for a specific route
   */
  async getRouteTrafficSummary(routeId: number, days: number = 7): Promise<{
    route_id: number;
    route_name: string;
    avg_traffic_density: number;
    peak_traffic_density: number;
    low_traffic_density: number;
    avg_speed_kmh: number;
    total_samples: number;
  } | null> {
    if (!this.trafficAnalyticsService) {
      throw new Error('Traffic analytics service not initialized');
    }

    return await this.trafficAnalyticsService.getRouteTrafficSummary(routeId, days);
  }

  /**
   * Check if traffic analytics service is available
   */
  isTrafficAnalyticsAvailable(): boolean {
    return !!this.trafficAnalyticsService;
  }
}