import yargs from 'yargs/yargs';
import { config } from './config'; // Import config
import { fetchLogs } from './services/elasticsearch'; // Import ES service
import { batchInsertUserRecords, batchInsertBukopinRecords, disconnectDb } from './services/database'; // Import DB service
import { extractPayloadFields } from './processing/logProcessor'; // Import processor
import { ProcessedLogDocument } from './types'; // Import type

// Define CLI arguments
const argv = yargs(process.argv.slice(2))
    .option('startDate', {
        alias: 's',
        description: 'Start date in YYYY-MM-DD format (inclusive, interpreted as GMT+08)',
        type: 'string',
        demandOption: true, // Make it required
    })
    .option('endDate', {
        alias: 'e',
        description: 'End date in YYYY-MM-DD format (exclusive, interpreted as GMT+08)',
        type: 'string',
        demandOption: true, // Make it required
    })
    .option('query', {
        alias: 'q',
        description: 'Query type to execute (retrieveUserInfo or bukopin)',
        type: 'string',
        default: 'retrieveUserInfo',
        choices: ['retrieveUserInfo', 'bukopin']
    })
    .help()
    .alias('help', 'h')
    .parseSync();

async function run() {
    // Use config for index pattern (though it's used within fetchLogs now)
    console.log(`Using index pattern: ${config.elastic.indexPattern}`);
    let exitCode = 0; // Default to success

    // --- DB Insert Variables ---
    const batchSize = 1000; // Adjust batch size as needed
    let processedBatch: ProcessedLogDocument[] = [];
    let totalProcessedCount = 0;
    let totalInsertedCount = 0;
    let totalSkippedPayloadCount = 0;
    let totalDbSkippedCount = 0; // Renamed for clarity (duplicates skipped by DB)
    // --- End DB Insert Variables ---

    const startTime = process.hrtime.bigint(); // Record start time

    try {
        // --- Calculate Date Range ---
        // Construct timezone-aware ISO strings for Elasticsearch
        const esStartDate = `${argv.startDate}T00:00:00+08:00`;
        const esEndDate = `${argv.endDate}T00:00:00+08:00`;

        // Use the elasticsearch service to fetch logs
        for await (const rawDocs of fetchLogs(esStartDate, esEndDate, argv.query)) {
            totalProcessedCount += rawDocs.length;
            console.log(`Fetched batch of ${rawDocs.length}. Total fetched: ${totalProcessedCount}`);

            // --- Optimization: Process documents in the current batch in parallel ---
            const processingPromises = rawDocs.map(rawDoc => 
                Promise.resolve(extractPayloadFields(rawDoc, argv.query)) // Pass query type to processor
            );
            const processedDocs = await Promise.all(processingPromises);

            // Filter out null results (skipped docs) and add to the main batch
            for (const processedDoc of processedDocs) {
                if (processedDoc) {
                    processedBatch.push(processedDoc);
                } else {
                    totalSkippedPayloadCount++;
                }
            }
            // --- End Optimization ---

            // Insert batch if it reaches the desired size
            if (processedBatch.length >= batchSize) {
                console.log(`Processing batch of ${processedBatch.length}...`);
                let result;
                if (argv.query === 'bukopin') {
                    result = await batchInsertBukopinRecords(processedBatch);
                } else {
                    result = await batchInsertUserRecords(processedBatch);
                }
                const { inserted, skipped } = result;
                totalInsertedCount += inserted;
                totalDbSkippedCount += skipped;
                console.log(`Batch processed: ${inserted} inserted, ${skipped} skipped by DB. Total inserted: ${totalInsertedCount}, Total DB skipped: ${totalDbSkippedCount}`);
                processedBatch = []; // Reset batch
            }
        }

        // Insert any remaining documents in the last batch
        if (processedBatch.length > 0) {
            console.log(`Processing final batch of ${processedBatch.length}...`);
            let result;
            if (argv.query === 'bukopin') {
                result = await batchInsertBukopinRecords(processedBatch);
            } else {
                result = await batchInsertUserRecords(processedBatch);
            }
            const { inserted, skipped } = result;
            totalInsertedCount += inserted;
            totalDbSkippedCount += skipped;
            console.log(`Final batch processed: ${inserted} inserted, ${skipped} skipped by DB.`);
            processedBatch = [];
        }

        const endTime = process.hrtime.bigint(); // Record end time
        const durationMs = Number(endTime - startTime) / 1_000_000;
        console.log(`\n--- Summary ---`);
        console.log(`Total execution time: ${durationMs.toFixed(2)} ms`);
        console.log(`Total documents processed from ES: ${totalProcessedCount}`);
        console.log(`Total payloads skipped during processing: ${totalSkippedPayloadCount}`);
        console.log(`Total records successfully inserted into DB: ${totalInsertedCount}`);
        console.log(`Total records skipped by DB (duplicates): ${totalDbSkippedCount}`);
        console.log('------------------------------------');

    } catch (error) {
        console.error("An error occurred during the process:", error);
        exitCode = 1;
    } finally {
        await disconnectDb(); // Ensure DB disconnection
        console.log(`Process finished with exit code ${exitCode}.`);
        process.exit(exitCode);
    }
}

run().catch(error => {
    // This catch might be redundant now due to the finally block in run,
    // but keep it as a safeguard for unexpected errors *before* run's try block.
    console.error("Unhandled error during script execution:", error);
    disconnectDb().finally(() => process.exit(1)); // Ensure disconnect even on initial error
});
