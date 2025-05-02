import { Client } from "@elastic/elasticsearch";
import { estypes } from "@elastic/elasticsearch";
import * as dotenv from 'dotenv';
import yargs from 'yargs/yargs'; 

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

    try {
        const result = await client.search<LogDocument>({
            index: indexPattern,
            size: 1, // Retrieve up to 50 matching documents
            _source: ["@timestamp", "uid", "payload"], // Specify fields to retrieve
            track_total_hits: true, // Add this line to get the exact count
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

        const totalHits = typeof result.hits.total === 'number'
            ? result.hits.total
            : result.hits.total?.value || 0;

        console.log(`Found ${totalHits} matching documents.`);

        if (result.hits.hits.length > 0) {
            console.log("\nRetrieved documents:");
            result.hits.hits.forEach((hit) => {
                console.log("----------------------------------------");
                if (hit._source) {
                    console.log(`Timestamp: ${hit._source["@timestamp"]}`);
                    console.log(`UID:       ${hit._source.uid}`);
                    console.log(`Payload:   ${JSON.stringify(hit._source.payload, null, 2)}`); 
                } else {
                    console.log("Document source is missing.");
                }
            });
        } else {
            console.log("No documents matched the specified criteria.");
        }
    } catch (error) {
        console.error("Error executing search:", error);
        const errorBody = (error as any)?.meta?.body;
        if (errorBody) {
            console.error("Elasticsearch error details:", JSON.stringify(errorBody, null, 2));
        }
        exitCode = 1; // Set exit code to error
    } finally {
        // Ensure client connection is closed before exiting
        console.log("Closing Elasticsearch client connection...");
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
