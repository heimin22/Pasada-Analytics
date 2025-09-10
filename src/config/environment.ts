import dotenv from 'dotenv';
import { QuestDBConfig } from '../services/questdbServices';

dotenv.config();

export interface EnvironmentConfig {
  // Server Configuration
  port: number;
  nodeEnv: string;
  apiUrl: string;

  // Google Maps
  googleMapsApiKey: string;

  // Supabase
  supabaseUrl: string;
  supabaseServiceRoleKey: string;
  supabaseAnonKey: string;
  postgresConnection: string;

  // QuestDB Configuration
  questdb: QuestDBConfig;

  // Analytics Configuration
  analytics: {
    batchSize: number;
    timeout: number;
  };

  // Logging Configuration
  logging: {
    level: string;
    fileMaxSize: string;
    fileMaxFiles: number;
  };
}

export class EnvironmentValidator {
  private static getEnvVar(name: string, defaultValue?: string): string {
    const value = process.env[name] || defaultValue;
    if (!value) {
      throw new Error(`Missing required environment variable: ${name}`);
    }
    return value;
  }

  private static getEnvNumber(name: string, defaultValue?: number): number {
    const value = process.env[name];
    if (!value) {
      if (defaultValue !== undefined) return defaultValue;
      throw new Error(`Missing required environment variable: ${name}`);
    }
    const parsed = parseInt(value, 10);
    if (isNaN(parsed)) {
      throw new Error(`Environment variable ${name} must be a number, got: ${value}`);
    }
    return parsed;
  }

  static validate(): EnvironmentConfig {
    try {
      const config: EnvironmentConfig = {
        // Server Configuration
        port: this.getEnvNumber('PORT', 3001),
        nodeEnv: this.getEnvVar('NODE_ENV', 'development'),
        apiUrl: this.getEnvVar('API_URL'),

        // Google Maps
        googleMapsApiKey: this.getEnvVar('GOOGLE_MAPS_API_KEY'),

        // Supabase
        supabaseUrl: this.getEnvVar('SUPABASE_URL'),
        supabaseServiceRoleKey: this.getEnvVar('SUPABASE_SERVICE_ROLE_KEY'),
        supabaseAnonKey: this.getEnvVar('SUPABASE_ANON_KEY'),
        postgresConnection: this.getEnvVar('POSTGRES_CONNECTION'),

        // QuestDB Configuration
        questdb: {
          pgConnectionString: this.getEnvVar('PG_CONN', process.env.DATABASE_URL || ''),
          ilpEndpoint: this.getEnvVar('QUESTDB_ILP'),
          httpEndpoint: this.getEnvVar('QUESTDB_HTTP'),
          connectionTimeout: this.getEnvNumber('QUESTDB_TIMEOUT', 30000),
        },

        // Analytics Configuration
        analytics: {
          batchSize: this.getEnvNumber('ANALYTICS_BATCH_SIZE', 100),
          timeout: this.getEnvNumber('ANALYTICS_TIMEOUT', 30000),
        },

        // Logging Configuration
        logging: {
          level: this.getEnvVar('LOG_LEVEL', 'info'),
          fileMaxSize: this.getEnvVar('LOG_FILE_MAX_SIZE', '10m'),
          fileMaxFiles: this.getEnvNumber('LOG_FILE_MAX_FILES', 7),
        },
      };

      // Validate QuestDB configuration
      if (!config.questdb.pgConnectionString) {
        throw new Error('Missing PostgreSQL connection string (PG_CONN or DATABASE_URL)');
      }

      console.log('Environment configuration validated successfully');
      return config;
    } catch (error) {
      console.error('Environment validation failed:', error);
      throw error;
    }
  }

  static validateOptional(): Partial<EnvironmentConfig> {
    // For cases where some env vars might be missing during development
    const config: Partial<EnvironmentConfig> = {};
    
    try {
      config.port = this.getEnvNumber('PORT', 3001);
      config.nodeEnv = this.getEnvVar('NODE_ENV', 'development');
    } catch (error) {
      console.warn('Some environment variables are missing:', error);
    }

    return config;
  }
}

// Export singleton instance
export const env = EnvironmentValidator.validate();
