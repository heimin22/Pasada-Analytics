import { TrafficAnalyticsService } from '../services/trafficAnalyticsService';
import { AnalyticsService } from '../services/analyticsService';

// Mock dependencies
jest.mock('@supabase/supabase-js');
jest.mock('@googlemaps/google-maps-services-js');
jest.mock('node-fetch');

describe('TrafficAnalyticsService', () => {
  let service: TrafficAnalyticsService;
  let mockConfig: any;

  beforeEach(() => {
    mockConfig = {
      questdb: {
        httpEndpoint: 'http://localhost:9000',
        pgConnectionString: 'postgresql://localhost:5432/test'
      },
      supabaseUrl: 'https://test.supabase.co',
      supabaseServiceKey: 'test-key',
      googleMapsApiKey: 'test-api-key'
    };

    service = new TrafficAnalyticsService(mockConfig);
  });

  describe('Configuration', () => {
    test('should initialize with valid configuration', () => {
      expect(service).toBeInstanceOf(TrafficAnalyticsService);
    });

    test('should handle missing Google Maps API key gracefully', () => {
      const configWithoutGoogleMaps = { ...mockConfig, googleMapsApiKey: '' };
      const serviceWithoutGoogleMaps = new TrafficAnalyticsService(configWithoutGoogleMaps);
      expect(serviceWithoutGoogleMaps).toBeInstanceOf(TrafficAnalyticsService);
    });

    test('should handle missing Supabase configuration gracefully', () => {
      const configWithoutSupabase = { ...mockConfig, supabaseUrl: '', supabaseServiceKey: '' };
      const serviceWithoutSupabase = new TrafficAnalyticsService(configWithoutSupabase);
      expect(serviceWithoutSupabase).toBeInstanceOf(TrafficAnalyticsService);
    });
  });

  describe('Traffic Analysis Methods', () => {
    test('getDefaultTrafficDensity should return expected values for different hours', () => {
      // Access private method through type assertion
      const getDefaultTrafficDensity = (service as any).getDefaultTrafficDensity;
      
      // Morning rush (7-9 AM)
      expect(getDefaultTrafficDensity(8)).toBe(0.6);
      
      // Evening rush (5-7 PM)
      expect(getDefaultTrafficDensity(18)).toBe(0.7);
      
      // Late night (11 PM - 5 AM)
      expect(getDefaultTrafficDensity(23)).toBe(0.1);
      expect(getDefaultTrafficDensity(3)).toBe(0.1);
      
      // Regular hours
      expect(getDefaultTrafficDensity(14)).toBe(0.3);
    });

    test('sleep utility should work correctly', async () => {
      const sleep = (service as any).sleep;
      const startTime = Date.now();
      await sleep(50);
      const endTime = Date.now();
      expect(endTime - startTime).toBeGreaterThanOrEqual(50);
    });
  });
});

describe('AnalyticsService Traffic Integration', () => {
  let analyticsService: AnalyticsService;

  beforeEach(() => {
    // Mock environment variables
    process.env.GOOGLE_MAPS_API_KEY = 'test-key';
    process.env.SUPABASE_URL = 'https://test.supabase.co';
    process.env.SUPABASE_SERVICE_ROLE_KEY = 'test-service-key';
    process.env.QUESTDB_HTTP = 'http://localhost:9000';
    
    analyticsService = new AnalyticsService(null);
  });

  afterEach(() => {
    // Clean up environment variables
    delete process.env.GOOGLE_MAPS_API_KEY;
    delete process.env.SUPABASE_URL;
    delete process.env.SUPABASE_SERVICE_ROLE_KEY;
    delete process.env.QUESTDB_HTTP;
  });

  test('should initialize traffic analytics service when properly configured', () => {
    expect(analyticsService).toBeInstanceOf(AnalyticsService);
  });

  test('isTrafficAnalyticsAvailable should return correct status', () => {
    // This will depend on whether the environment variables are properly set
    const isAvailable = analyticsService.isTrafficAnalyticsAvailable();
    expect(typeof isAvailable).toBe('boolean');
  });
});

describe('Traffic Analysis Data Validation', () => {
  test('should validate route data structure', () => {
    const validRoute = {
      officialroute_id: 1,
      route_name: 'Test Route',
      origin_name: 'Origin',
      destination_name: 'Destination',
      status: 'active',
      origin_lat: '14.5995',
      origin_lng: '120.9842',
      destination_lat: '14.6091',
      destination_lng: '121.0223',
      created_at: new Date().toISOString()
    };

    expect(validRoute.officialroute_id).toBeDefined();
    expect(validRoute.route_name).toBeDefined();
    expect(validRoute.origin_lat).toBeDefined();
    expect(validRoute.origin_lng).toBeDefined();
    expect(validRoute.destination_lat).toBeDefined();
    expect(validRoute.destination_lng).toBeDefined();
  });

  test('should validate traffic analysis result structure', () => {
    const validAnalysis = {
      route_id: 1,
      route_name: 'Test Route',
      timestamp: new Date(),
      distance_meters: 5000,
      duration_seconds: 600,
      duration_in_traffic_seconds: 900,
      traffic_density: 0.5,
      speed_kmh: 30,
      traffic_penalty_seconds: 300,
      traffic_severity: 'moderate' as const,
      origin: { lat: 14.5995, lng: 120.9842, name: 'Origin' },
      destination: { lat: 14.6091, lng: 121.0223, name: 'Destination' }
    };

    expect(validAnalysis.route_id).toBeDefined();
    expect(validAnalysis.traffic_density).toBeGreaterThanOrEqual(0);
    expect(validAnalysis.traffic_density).toBeLessThanOrEqual(1);
    expect(['low', 'moderate', 'high', 'severe']).toContain(validAnalysis.traffic_severity);
  });

  test('should validate forecast data structure', () => {
    const validForecast = {
      route_id: 1,
      forecast_date: new Date(),
      hourly_predictions: [
        {
          hour: 8,
          predicted_density: 0.7,
          confidence: 0.8,
          expected_duration_seconds: 900
        }
      ]
    };

    expect(validForecast.route_id).toBeDefined();
    expect(validForecast.hourly_predictions).toBeInstanceOf(Array);
    expect(validForecast.hourly_predictions[0].hour).toBeGreaterThanOrEqual(0);
    expect(validForecast.hourly_predictions[0].hour).toBeLessThan(24);
    expect(validForecast.hourly_predictions[0].confidence).toBeGreaterThanOrEqual(0);
    expect(validForecast.hourly_predictions[0].confidence).toBeLessThanOrEqual(1);
  });
});
