import { QuestDBService } from './questdbServices';
import { WeeklyAnalyticsService } from './weeklyAnalyticsService';
import { TrafficData, TrafficPrediction } from '../types/traffic';
import { DatabaseService } from '../types/services';

export class AnalyticsService {
  private questdbService?: QuestDBService;
  private weeklyAnalyticsService?: WeeklyAnalyticsService;

  constructor(
    private databaseService: DatabaseService | null
  ) {
    // Initialize QuestDB services if configured
    this.initializeQuestDB();
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
}