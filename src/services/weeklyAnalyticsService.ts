import { QuestDBService, QuestDBConfig } from '../services/questdbServices';
import { TrafficPrediction, RouteAnalyticsSummary } from '../types/traffic';

export class WeeklyAnalyticsService {
  private questdbService: QuestDBService;

  constructor(config: QuestDBConfig) {
    this.questdbService = new QuestDBService(config);
  }

  async processWeeklyAnalytics(weekOffset: number = 1): Promise<{
    success: boolean;
    rowsProcessed: number;
    message: string;
  }> {
    try {
      await this.questdbService.connect();

      // Get weekly data with SQL injection protection
      const weeklyData = await this.questdbService.getWeeklyAnalytics(weekOffset);

      if (weeklyData.length === 0) {
        return {
          success: true,
          rowsProcessed: 0,
          message: 'No data found for the specified week'
        };
      }

      // Push to QuestDB
      await this.questdbService.pushToQuestDB(weeklyData);

      return {
        success: true,
        rowsProcessed: weeklyData.length,
        message: `Successfully processed ${weeklyData.length} weekly analytics records`
      };
    } catch (error) {
      console.error('Weekly analytics processing failed:', error);
      return {
        success: false,
        rowsProcessed: 0,
        message: `Failed to process weekly analytics: ${error}`
      };
    } finally {
      await this.questdbService.disconnect();
    }
  }

  // Generate 7-day traffic predictions based on historical patterns
  async generateTrafficPredictions(routeId: number): Promise<TrafficPrediction[]> {
    const query = `
      SELECT 
        extract(hour from timestamp) as hour,
        avg(traffic_density) as avg_density,
        count(*) as data_points
      FROM traffic_analytics 
      WHERE route_id = ${routeId} 
        AND timestamp > dateadd('w', -4, now())
      GROUP BY hour
      ORDER BY hour
    `;

    try {
      await this.questdbService.connect();
      const historicalData = await this.questdbService.queryAnalytics(query, { route_id: routeId.toString() });
      
      return this.createPredictions(historicalData);
    } catch (error) {
      console.error('Failed to generate predictions:', error);
      throw error;
    } finally {
      await this.questdbService.disconnect();
    }
  }

  private createPredictions(historicalData: string[][]): TrafficPrediction[] {
    const predictions: TrafficPrediction[] = [];
    const now = new Date();

    for (let day = 1; day <= 7; day++) {
      const predictionDate = new Date(now);
      predictionDate.setDate(now.getDate() + day);

      // Generate predictions for key hours
      [7, 9, 12, 17, 19, 22].forEach(hour => {
        // Find matching historical data row
        const historicalRow = historicalData.find(row => {
          const hourValue = parseInt(row[0]); // hour is first column
          return hourValue === hour;
        });
        
        const predictedDensity = historicalRow 
          ? parseFloat(historicalRow[1]) // avg_density is second column
          : this.getDefaultDensityForHour(hour, predictionDate.getDay());
        
        const dataPoints = historicalRow ? parseInt(historicalRow[2]) : 0; // data_points is third column
        const confidence = historicalRow && dataPoints > 5 
          ? Math.min(0.9, 0.3 + (dataPoints * 0.1)) 
          : 0.3;

        predictions.push({
          date: new Date(predictionDate.getFullYear(), predictionDate.getMonth(), predictionDate.getDate(), hour),
          predictedDensity,
          confidence,
          timeOfDay: `${hour.toString().padStart(2, '0')}:00`
        });
      });
    }

    return predictions;
  }

  private getDefaultDensityForHour(hour: number, dayOfWeek: number): number {
    const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;
    let baseDensity = 0.3;

    if (hour >= 7 && hour <= 9) {
      baseDensity = isWeekend ? 0.4 : 0.7; // Morning rush
    } else if (hour >= 17 && hour <= 19) {
      baseDensity = isWeekend ? 0.5 : 0.8; // Evening rush
    } else if (hour >= 22 || hour <= 6) {
      baseDensity = 0.2; // Late night/early morning
    }

    return baseDensity;
  }

  async getRouteAnalyticsSummary(routeId: number, days: number = 30): Promise<RouteAnalyticsSummary | null> {
    const query = `
      SELECT 
        route_id,
        avg(avg_traffic_density) as overall_avg_density,
        max(max_traffic_density) as peak_density,
        min(min_traffic_density) as lowest_density,
        avg(avg_speed_kmh) as avg_speed,
        sum(sample_count) as total_samples
      FROM route_weekly_summary 
      WHERE route_id = '${routeId}' 
        AND ts > dateadd('d', -${days}, now())
    `;

    try {
      await this.questdbService.connect();
      const result = await this.questdbService.queryAnalytics(query.replace('$1', routeId.toString()));
      
      if (result.length === 0) {
        return null;
      }
      
      const row = result[0];
      return {
        route_id: parseInt(row[0]),
        overall_avg_density: parseFloat(row[1]),
        peak_density: parseFloat(row[2]),
        lowest_density: parseFloat(row[3]),
        avg_speed: parseFloat(row[4]),
        total_samples: parseInt(row[5])
      };
    } finally {
      await this.questdbService.disconnect();
    }
  }
}