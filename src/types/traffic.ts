export interface TrafficPrediction {
  date: Date;
  predictedDensity: number;
  confidence: number;
  timeOfDay: string;
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

export interface HistoricalTrafficData {
  hour: number;
  avg_density: number;
  data_points: number;
}

export interface RouteAnalyticsSummary {
  route_id: number;
  overall_avg_density: number;
  peak_density: number;
  lowest_density: number;
  avg_speed: number;
  total_samples: number;
}
