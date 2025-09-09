import { TrafficData } from './traffic';

export interface DatabaseService {
  saveTrafficData(trafficData: TrafficData[]): Promise<void>;
}

export interface GeminiService {
  // Add methods as needed
}

export interface GoogleMapsService {
  // Add methods as needed
}
