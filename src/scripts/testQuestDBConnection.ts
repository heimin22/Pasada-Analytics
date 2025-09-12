#!/usr/bin/env ts-node
import { QuestDBService } from '../services/questdbServices';
import { env } from '../config/environment';
import logger, { analyticsLogger } from '../utils/logger';

async function testQuestDBConnection() {
  console.log('Testing QuestDB Connection...\n');
  
  try {
    // Test configuration
    console.log('Configuration:');
    console.log(`   PostgreSQL: ${env.questdb.pgConnectionString ? 'Configured' : 'Missing'}`);
    console.log(`   ILP Endpoint: ${env.questdb.ilpEndpoint || 'Missing'}`);
    console.log(`   HTTP Endpoint: ${env.questdb.httpEndpoint || 'Missing'}`);
    console.log('');

    const questdbService = new QuestDBService(env.questdb);

    // Test 1: PostgreSQL Connection
    console.log('Testing PostgreSQL connection...');
    await questdbService.connect();
    console.log('PostgreSQL connection successful');
    
    // Test 2: Basic Query
    console.log('Testing basic query...');
    const testQuery = `
      SELECT 
        COUNT(*) as total_records,
        MIN(timestamp) as earliest_record,
        MAX(timestamp) as latest_record
      FROM traffic_analytics 
      LIMIT 1
    `;
    
    try {
      const result = await questdbService.queryAnalytics(testQuery);
      console.log('Query executed successfully');
      console.log('   Results:', result);
    } catch (queryError) {
      console.log('Query failed (table might not exist):', queryError);
    }

    // Test 3: Table Structure Check
    console.log('Checking table structure...');
    try {
      const structureQuery = `
        SELECT table_name, column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name IN ('traffic_analytics', 'route_weekly_summary')
        ORDER BY table_name, ordinal_position
      `;
      const structure = await questdbService.queryAnalytics(structureQuery);
      
      if (structure.length > 0) {
        console.log('Table structure:');
        let currentTable = '';
        structure.forEach(row => {
          if (row[0] !== currentTable) {
            currentTable = row[0];
            console.log(`\n    ${currentTable}:`);
          }
          console.log(`      ${row[1]} (${row[2]})`);
        });
      } else {
        console.log('No tables found or structure query failed');
      }
    } catch (structureError) {
      console.log('Could not check table structure:', structureError);
    }

    // Test 4: ILP Write Test (if endpoints available)
    if (env.questdb.ilpEndpoint) {
      console.log('\nTesting ILP write...');
      const testData = [{
        week_start: '2024-01-01',
        route_id: 999,
        sample_count: 1,
        avg_traffic_density: 0.5,
        min_traffic_density: 0.1,
        max_traffic_density: 0.9,
        avg_duration_seconds: 300,
        avg_duration_in_traffic_seconds: 450,
        avg_traffic_penalty_fraction: 0.5,
        total_distance_meters: 5000,
        avg_speed_kmh: 60,
        peak_hour: 8
      }];

      try {
        await questdbService.pushToQuestDB(testData);
        console.log('ILP write test successful');
      } catch (ilpError) {
        console.log('ILP write test failed:', ilpError);
      }
    } else {
      console.log('Skipping ILP test (no endpoint configured)');
    }

    // Cleanup
    await questdbService.disconnect();
    
    console.log('\nQuestDB connection test completed successfully!');
    analyticsLogger.questdb('Connection test completed successfully');
    
  } catch (error) {
    console.error('\nQuestDB connection test failed:', error);
    analyticsLogger.error('QuestDB connection test failed', error as Error);
    process.exit(1);
  }
}

// Performance test
async function performanceTest() {
  console.log('\nRunning performance test...');
  
  const questdbService = new QuestDBService(env.questdb);
  const startTime = Date.now();
  
  try {
    await questdbService.connect();
    
    // Test multiple queries
    const queries = [
      "SELECT COUNT(*) FROM traffic_analytics",
      "SELECT DISTINCT route_id FROM traffic_analytics LIMIT 10",
      "SELECT AVG(traffic_density) FROM traffic_analytics WHERE timestamp > dateadd('d', -7, now())"
    ];
    
    for (const query of queries) {
      const queryStart = Date.now();
      try {
        await questdbService.queryAnalytics(query);
        console.log(`   Query completed in ${Date.now() - queryStart}ms`);
      } catch (error) {
        console.log(`   Query failed: ${error}`);
      }
    }
    
    await questdbService.disconnect();
    
    const totalTime = Date.now() - startTime;
    console.log(`Performance test completed in ${totalTime}ms`);
    
  } catch (error) {
    console.error('Performance test failed:', error);
  }
}

async function main() {
  try {
    await testQuestDBConnection();
    
    if (process.argv.includes('--performance')) {
      await performanceTest();
    }
    
  } catch (error) {
    logger.error('Test script failed', error);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}
