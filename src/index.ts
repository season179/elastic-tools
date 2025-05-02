import { Client } from "@elastic/elasticsearch";
import { estypes } from "@elastic/elasticsearch";
import dotenv from "dotenv";

// Load environment variables from .env file
dotenv.config();

// Initialize the Elasticsearch client
// There are two ways to connect to Elasticsearch:
// 1. Using ELASTIC_NODE (URL) and ELASTIC_API_KEY
// 2. Using ELASTIC_CLOUD_ID and ELASTIC_API_KEY
//
// This example uses the first approach with the environment variables from .env

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
    message?: string;   // Optional: Field likely containing "RetrieveUserInfo"
    action?: string;    // Optional: Field likely containing "response"
    // Add other relevant fields if known
}

async function run() {
    const indexPattern = "prod-my-storage-logs-alias";
    console.log(`Querying index pattern: ${indexPattern}`);

    try {
        const result = await client.search<LogDocument>({
            index: indexPattern,
            size: 50, // Retrieve up to 50 matching documents
            _source: ["@timestamp", "uid", "payload"], // Specify fields to retrieve
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
                                    gte: "2025-02-01T00:00:00.000+08:00", // Start date in UTC+08:00
                                    lte: "2025-02-05T00:00:00.000+08:00"  // End date in UTC+08:00
                                    // Using ISO 8601 format with +08:00 offset
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
    }
}

run().catch(console.log);
