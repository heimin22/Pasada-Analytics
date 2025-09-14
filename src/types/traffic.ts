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

// Enhanced interfaces for traffic analytics
export interface OfficialRoute {
  officialroute_id: number;
  route_name: string;
  origin_name: string;
  destination_name: string;
  description?: string;
  status: string;
  origin_lat?: string;
  origin_lng?: string;
  destination_lat?: string;
  destination_lng?: string;
  intermediate_coordinates?: any;
  created_at: string;
}

export interface AllowedStop {
  allowedstop_id: number;
  officialroute_id: number;
  stop_name: string;
  stop_address: string;
  stop_lat: string;
  stop_lng: string;
  stop_order?: number;
  is_active: boolean;
  created_at: string;
}

export interface GoogleMapsTrafficData {
  distance: {
    text: string;
    value: number; // in meters
  };
  duration: {
    text: string;
    value: number; // in seconds
  };
  duration_in_traffic: {
    text: string;
    value: number; // in seconds
  };
  traffic_speed_entry?: any[];
  via_waypoint?: any[];
}

export interface RouteTrafficAnalysis {
  route_id: number;
  route_name: string;
  timestamp: Date;
  distance_meters: number;
  duration_seconds: number;
  duration_in_traffic_seconds: number;
  traffic_density: number; // 0-1 scale
  speed_kmh: number;
  traffic_penalty_seconds: number;
  traffic_severity: 'low' | 'moderate' | 'high' | 'severe';
  origin: {
    lat: number;
    lng: number;
    name: string;
  };
  destination: {
    lat: number;
    lng: number;
    name: string;
  };
}

export interface WeeklyTrafficReport {
  route_id: number;
  week_start: Date;
  total_samples: number;
  avg_traffic_density: number;
  peak_traffic_density: number;
  low_traffic_density: number;
  avg_speed_kmh: number;
  peak_congestion_hour: number;
  improvement_suggestions: string[];
}

export interface TrafficForecast {
  route_id: number;
  forecast_date: Date;
  hourly_predictions: {
    hour: number;
    predicted_density: number;
    confidence: number;
    expected_duration_seconds: number;
  }[];
}
