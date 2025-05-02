import { Client } from "@elastic/elasticsearch";
import { estypes } from "@elastic/elasticsearch";
import * as dotenv from 'dotenv';
import yargs from 'yargs/yargs'; 
import * as fs from 'fs';
import * as path from 'path';

// Load environment variables from .env file
dotenv.config();

// Define CLI arguments
const argv = yargs(process.argv.slice(2))
  .option('startDate', {
    alias: 's',
    description: 'Start date in YYYY-MM-DD format (inclusive)',
    type: 'string',
    demandOption: true, // Make it required
  })
  .option('endDate', {
    alias: 'e',
    description: 'End date in YYYY-MM-DD format (exclusive)',
    type: 'string',
    demandOption: true, // Make it required
  })
  .help()
  .alias('help', 'h')
  .parseSync(); 

// Check for required environment variables
if (!process.env.ELASTIC_CLOUD_ID || !process.env.ELASTIC_API_KEY) {
    throw new Error(
        "Missing required environment variables: ELASTIC_CLOUD_ID and/or ELASTIC_API_KEY"
    );
}

const client = new Client({
    cloud: { id: process.env.ELASTIC_CLOUD_ID as string },
    auth: { apiKey: process.env.ELASTIC_API_KEY as string },
});

// Alternative configuration using Node URL:
// const client = new Client({
//   node: process.env.ELASTIC_NODE,
//   auth: { apiKey: process.env.ELASTIC_API_KEY }
// })

// Define the structure of the documents we expect to retrieve
interface LogDocument {
    "@timestamp": string; // Elasticsearch timestamp field
    uid: string;        // User ID field
    payload: any;       // Payload field (can be complex)
    // Add other relevant fields if known
}

async function run() {
    // Use environment variable for the index pattern
    const indexPattern = process.env.ELASTIC_INDEX_PATTERN;
    if (!indexPattern) {
        console.error("Error: ELASTIC_INDEX_PATTERN environment variable is not set.");
        process.exit(1); // Exit if the crucial variable is missing
    }

    console.log(`Querying index pattern: ${indexPattern}`);
    let exitCode = 0; // Default to success
    let scrollId: string | undefined = undefined; // To store scroll ID
    const allHits: LogDocument[] = []; // To accumulate all hits

    try {
        // Initial search request with scroll parameter
        const response = await client.search<LogDocument>({
            index: indexPattern,
            scroll: '1m', // Keep the search context alive for 1 minute
            size: 5000, // Fetch 1000 docs per scroll page
            _source: ["@timestamp", "uid", "payload"],
            track_total_hits: true,
            query: {
                bool: {
                    must: [
                        // Revert to 'match' query for 'module'
                        { match: { module: "RetrieveUserInfo" } },
                        // Use 'match' query for potentially analyzed 'action' field
                        { match: { action: "response" } }
                    ],
                    filter: [
                        {
                            range: {
                                "@timestamp": {
                                    gte: argv.startDate, // Use start date from CLI argument
                                    lt: argv.endDate    // Use end date from CLI argument
                                }
                            }
                        }
                    ]
                },
            },
            sort: [
                { "@timestamp": { order: "desc" } } // Sort by timestamp, newest first
            ]
        });

        let currentHits = response.hits.hits;
        scrollId = response._scroll_id;

        // Accumulate initial batch of hits
        currentHits.forEach(hit => {
            if (hit._source) {
                allHits.push(hit._source);
            }
        });

        console.log(`Initial fetch: ${currentHits.length} hits.`);

        // Loop through subsequent pages using the scroll API
        while (scrollId && currentHits.length > 0) {
            console.log(`Fetching next batch with scroll_id: ${scrollId.substring(0, 10)}...`);
            const scrollResponse = await client.scroll<LogDocument>({
                scroll_id: scrollId,
                scroll: '1m'
            });

            scrollId = scrollResponse._scroll_id; // Update scroll ID for the next iteration
            currentHits = scrollResponse.hits.hits;

            if (currentHits.length > 0) {
                console.log(`Fetched ${currentHits.length} more hits.`);
                currentHits.forEach(hit => {
                    if (hit._source) {
                        allHits.push(hit._source);
                    }
                });
            } else {
                console.log("No more hits to fetch.");
            }
        }

        console.log(`\nTotal documents retrieved: ${allHits.length}`);

        // ---- CSV Writing Logic Start (after collecting all hits) ----
        if (allHits.length > 0) {
            const dataDir = path.join(__dirname, '..', 'data');
            if (!fs.existsSync(dataDir)) {
                fs.mkdirSync(dataDir, { recursive: true });
                console.log(`Created data directory: ${dataDir}`);
            }

            const fileName = `${argv.startDate}-${argv.endDate}.csv`; // Updated filename
            const filePath = path.join(dataDir, fileName);

            const headers = ['"@timestamp"', '"uid"', '"payload"'].join(',');
            const rows = allHits.map(source => {
                const timestamp = source["@timestamp"] || '';
                const uid = source.uid || '';
                const payload = source.payload ? `"${JSON.stringify(source.payload).replace(/"/g, '""')}"` : '""';
                return [timestamp, uid, payload].join(',');
            });

            const csvContent = `${headers}\n${rows.join('\n')}`;
            fs.writeFileSync(filePath, csvContent);
            console.log(`All ${allHits.length} results saved to ${filePath}`);
        } else {
            console.log("No documents matched the specified criteria to save.");
        }
        // ---- CSV Writing Logic End ----
    } catch (error) {
        console.error("Error executing search or scroll:", error);
        const errorBody = (error as any)?.meta?.body;
        if (errorBody) {
            console.error("Elasticsearch error details:", JSON.stringify(errorBody, null, 2));
        }
        exitCode = 1; // Set exit code to error
    } finally {
        // Ensure client connection is closed before exiting
        console.log("Closing Elasticsearch client connection...");
        // Clear the scroll context if it exists
        if (scrollId) {
            try {
                console.log(`Clearing scroll context: ${scrollId.substring(0, 10)}...`);
                await client.clearScroll({
                    scroll_id: scrollId,
                });
                console.log("Scroll context cleared.");
            } catch (clearScrollError) {
                console.error("Error clearing scroll context:", clearScrollError);
                // Don't necessarily exit with error 1 just because clearScroll failed
            }
        }
        try {
            await client.close(); // Attempt to close the client
            console.log("Client closed.");
        } catch (closeError) {
            console.error("Error closing Elasticsearch client:", closeError);
            exitCode = 1; // Ensure we exit with an error code if closing fails
        }
        process.exit(exitCode); // Exit with the appropriate code
    }
}

run().catch(console.log);
