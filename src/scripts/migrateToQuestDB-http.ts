#!/usr/bin/env ts-node
import { QuestDBServiceHTTP } from '../services/questdbServices-http';
import { env } from '../config/environment';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import logger, { analyticsLogger } from '../utils/logger';

interface MigrationOptions {
  dryRun: boolean;
  batchSize: number;
  startDate?: string | undefined;
  endDate?: string | undefined;
  routeIds?: number[] | undefined;
}

class QuestDBMigrationHTTP {
  private questdbService: QuestDBServiceHTTP;
  private supabase: SupabaseClient;

  constructor() {
    this.questdbService = new QuestDBServiceHTTP(env.questdb);
    this.supabase = createClient(
      env.supabaseUrl,
      env.supabaseServiceRoleKey
    );
  }

  private async testQuestDBConnection(): Promise<void> {
    try {
      // Test QuestDB HTTP endpoint with a simple query
      const result = await this.questdbService.queryAnalytics('SELECT 1 as test');
      if (!result || result.length === 0) {
        throw new Error('QuestDB query returned no results');
      }
    } catch (error) {
      throw new Error(`QuestDB HTTP endpoint test failed: ${(error as Error).message}`);
    }
  }

  async migrate(options: MigrationOptions): Promise<void> {
    console.log('Starting QuestDB Migration (HTTP Version)...');
    console.log(`   Dry Run: ${options.dryRun ? 'Yes' : 'No'}`);
    console.log(`   Batch Size: ${options.batchSize}`);
    console.log(`   Date Range: ${options.startDate || 'All'} to ${options.endDate || 'All'}`);
    console.log('');

    try {
      // Test Supabase API connection
      const { count, error } = await this.supabase
        .from('traffic_analytics')
        .select('*', { count: 'exact', head: true });

      if (error) {
        throw new Error(`Supabase API connection failed: ${error.message}`);
      }

      console.log(`Connected to Supabase API - ${count} total records available`);

      // Test QuestDB HTTP endpoint connectivity
      try {
        await this.testQuestDBConnection();
        console.log('QuestDB HTTP endpoint is reachable');
      } catch (error) {
        console.warn('QuestDB connection test failed:', (error as Error).message);
        if (!options.dryRun) {
          throw new Error('QuestDB is not accessible - migration cannot proceed');
        }
      }

      // Step 1: Migrate traffic analytics data
      await this.migrateTrafficAnalytics(options);

      console.log('\nMigration completed successfully!');

    } catch (error) {
      console.error('\nMigration failed:', error);
      analyticsLogger.error('Migration failed', error as Error);
      throw error;
    }
  }

  private async migrateTrafficAnalytics(options: MigrationOptions): Promise<void> {
    console.log('Migrating traffic analytics data...');

    // Build query filters
    let query = this.supabase
      .from('traffic_analytics')
      .select(`
        route_id,
        timestamp,
        traffic_density,
        duration,
        duration_in_traffic,
        distance,
        status,
        created_at
      `);

    // Apply filters
    if (options.startDate) {
      query = query.gte('timestamp', options.startDate);
    }
    if (options.endDate) {
      query = query.lte('timestamp', options.endDate);
    }
    if (options.routeIds && options.routeIds.length > 0) {
      query = query.in('route_id', options.routeIds);
    }

    // Get total count
    const { count: totalRecords, error: countError } = await this.supabase
      .from('traffic_analytics')
      .select('*', { count: 'exact', head: true });

    if (countError) {
      throw new Error(`Failed to get record count: ${countError.message}`);
    }

    console.log(`   Total records to migrate: ${totalRecords}`);

    if (!totalRecords || totalRecords === 0) {
      console.log('   No records found to migrate');
      return;
    }

    // Process in batches
    let offset = 0;
    let totalMigrated = 0;

    while (offset < totalRecords) {
      // Build batch query with pagination
      let batchQuery = this.supabase
        .from('traffic_analytics')
        .select(`
          route_id,
          timestamp,
          traffic_density,
          duration,
          duration_in_traffic,
          distance,
          status,
          created_at
        `)
        .order('timestamp', { ascending: true })
        .range(offset, offset + options.batchSize - 1);

      // Apply same filters to batch query
      if (options.startDate) {
        batchQuery = batchQuery.gte('timestamp', options.startDate);
      }
      if (options.endDate) {
        batchQuery = batchQuery.lte('timestamp', options.endDate);
      }
      if (options.routeIds && options.routeIds.length > 0) {
        batchQuery = batchQuery.in('route_id', options.routeIds);
      }

      const { data: batchData, error: batchError } = await batchQuery;

      if (batchError) {
        throw new Error(`Failed to fetch batch: ${batchError.message}`);
      }

      if (!batchData || batchData.length === 0) {
        break;
      }

      console.log(`   Processing batch: ${offset + 1}-${offset + batchData.length} of ${totalRecords}`);

      if (!options.dryRun) {
        // Transform data for QuestDB
        const trafficData = batchData.map(row => ({
          timestamp: row.timestamp,
          routeId: row.route_id,
          trafficDensity: row.traffic_density,
          duration: row.duration,
          durationInTraffic: row.duration_in_traffic,
          distance: row.distance,
          status: row.status
        }));

        // Save to QuestDB using HTTP API
        await this.questdbService.saveTrafficDataToQuestDB(trafficData);
        analyticsLogger.trafficData('Migrated batch to QuestDB', trafficData.length, {
          offset,
          batchSize: batchData.length
        });
      }

      totalMigrated += batchData.length;
      offset += options.batchSize;

      // Progress indicator
      const progress = Math.round((offset / totalRecords) * 100);
      console.log(`   Progress: ${progress}% (${totalMigrated}/${totalRecords})`);

      // Small delay to prevent overwhelming the databases
      await new Promise(resolve => setTimeout(resolve, 200));
    }

    console.log(`Traffic analytics migration completed: ${totalMigrated} records`);
  }

  async validateMigration(): Promise<void> {
    console.log('Validating migration...');

    try {
      // Check record counts using HTTP API
      const trafficCountQuery = 'SELECT COUNT(*) as count FROM traffic_analytics';
      const trafficCount = await this.questdbService.queryAnalytics(trafficCountQuery);
      
      console.log(`   Traffic Analytics Records: ${trafficCount[0]?.[0] || 0}`);

      // Check data quality
      const qualityQuery = `
        SELECT 
          MIN(timestamp) as earliest,
          MAX(timestamp) as latest,
          COUNT(DISTINCT route_id) as unique_routes,
          AVG(traffic_density) as avg_density
        FROM traffic_analytics
      `;

      const quality = await this.questdbService.queryAnalytics(qualityQuery);
      const [earliest, latest, uniqueRoutes, avgDensity] = quality[0] || [];

      console.log(`   Date Range: ${earliest} to ${latest}`);
      console.log(`   Unique Routes: ${uniqueRoutes}`);
      console.log(`   Average Density: ${avgDensity}`);

      console.log('Migration validation completed');

    } catch (error) {
      console.error('Migration validation failed:', error);
      throw error;
    }
  }
}

async function main() {
  const args = process.argv.slice(2);
  
  const options: MigrationOptions = {
    dryRun: args.includes('--dry-run'),
    batchSize: parseInt(args.find(arg => arg.startsWith('--batch-size='))?.split('=')[1] || '50'),
    startDate: args.find(arg => arg.startsWith('--start-date='))?.split('=')[1] || undefined,
    endDate: args.find(arg => arg.startsWith('--end-date='))?.split('=')[1] || undefined,
    routeIds: args.find(arg => arg.startsWith('--route-ids='))?.split('=')[1]?.split(',').map(Number) || undefined
  };

  const migration = new QuestDBMigrationHTTP();

  try {
    if (args.includes('--validate-only')) {
      await migration.validateMigration();
    } else {
      await migration.migrate(options);
      
      if (args.includes('--validate')) {
        await migration.validateMigration();
      }
    }
  } catch (error) {
    logger.error('Migration script failed', error);
    process.exit(1);
  }
}

// Usage examples in comments
/*
Usage:
  ts-node src/scripts/migrateToQuestDB-http.ts                         # Full migration
  ts-node src/scripts/migrateToQuestDB-http.ts --dry-run               # Test run only
  ts-node src/scripts/migrateToQuestDB-http.ts --batch-size=50         # Custom batch size
  ts-node src/scripts/migrateToQuestDB-http.ts --route-ids=9           # Specific routes only
  ts-node src/scripts/migrateToQuestDB-http.ts --validate              # Migrate and validate
  ts-node src/scripts/migrateToQuestDB-http.ts --validate-only         # Validation only
*/

if (require.main === module) {
  main();
}

export default QuestDBMigrationHTTP;
