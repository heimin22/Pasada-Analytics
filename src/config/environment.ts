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

  private static getEnvVarOptional(name: string): string | undefined {
    return process.env[name];
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
    const missingVars: string[] = [];
    const warnings: string[] = [];

    try {
      const nodeEnv = process.env.NODE_ENV;
      
      const config: EnvironmentConfig = {
        // Server Configuration - always required
        port: this.getEnvNumber('PORT', 3001),
        nodeEnv: this.getEnvVar('NODE_ENV', 'development'),
        apiUrl: this.getEnvVarOptional('API_URL') || `http://localhost:${this.getEnvNumber('PORT', 3001)}`,

        // Google Maps - optional in development
        googleMapsApiKey: this.getEnvVarOptional('GOOGLE_MAPS_API_KEY') || '',

        // Supabase - required in production, optional in development
        supabaseUrl: this.getEnvVarOptional('SUPABASE_URL') || '',
        supabaseServiceRoleKey: this.getEnvVarOptional('SUPABASE_SERVICE_ROLE_KEY') || '',
        supabaseAnonKey: this.getEnvVarOptional('SUPABASE_ANON_KEY') || '',
        postgresConnection: this.getEnvVarOptional('POSTGRES_CONNECTION') || '',

        // QuestDB Configuration - check if at least basic endpoints are available
        questdb: {
          pgConnectionString: this.getEnvVarOptional('PG_CONN') || process.env.SUPABASE_URL || '',
          ilpEndpoint: this.getEnvVarOptional('QUESTDB_ILP') || '',
          httpEndpoint: this.getEnvVarOptional('QUESTDB_HTTP') || '',
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

      // Validation based on environment
      if (nodeEnv === 'production') {
        // In production, require essential services
        if (!config.questdb.pgConnectionString) {
          missingVars.push('PG_CONN or SUPABASE_URL');
        }
        if (!config.questdb.ilpEndpoint) {
          missingVars.push('QUESTDB_ILP');
        }
        if (!config.questdb.httpEndpoint) {
          missingVars.push('QUESTDB_HTTP');
        }
        if (!config.supabaseUrl) {
          warnings.push('SUPABASE_URL not set - some features may not work');
        }
      } else {
        // In development, just warn about missing services
        if (!config.questdb.pgConnectionString) {
          warnings.push('Database connection not configured (PG_CONN/SUPABASE_URL)');
        }
        if (!config.questdb.ilpEndpoint) {
          warnings.push('QuestDB ILP endpoint not configured (QUESTDB_ILP)');
        }
        if (!config.questdb.httpEndpoint) {
          warnings.push('QuestDB HTTP endpoint not configured (QUESTDB_HTTP)');
        }
      }

      // Show warnings
      if (warnings.length > 0) {
        console.warn('Environment warnings:');
        warnings.forEach(warning => console.warn(`${warning}`));
      }

      // Fail only if critical variables are missing
      if (missingVars.length > 0) {
        const errorMsg = `Missing required environment variables: ${missingVars.join(', ')}`;
        console.error(`${errorMsg}`);
        throw new Error(errorMsg);
      }

      console.log(`Environment configuration validated successfully (${nodeEnv} mode)`);
      if (warnings.length === 0) {
        console.log('All services properly configured');
      }
      
      return config;
    } catch (error) {
      console.error('Environment validation failed:', error);
      console.error('Check ENVIRONMENT.md for setup instructions');
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
