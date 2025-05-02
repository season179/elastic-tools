import { Client, estypes } from '@elastic/elasticsearch';
import dotenv from 'dotenv';

dotenv.config(); // Load environment variables from .env file

// Define an interface for your document structure (optional but recommended)
interface PaywatchLog {
  '@timestamp': string; // Or Date
  action?: string;
  payload?: any; // Be more specific if possible, e.g., Record<string, unknown> or a dedicated interface
  aid?: string;
  // Add other fields present in your documents
  [key: string]: any; // Allow other fields
}

// --- Configuration ---
// IMPORTANT: Use environment variables or a secure config management system
// DO NOT HARDCODE CREDENTIALS IN YOUR CODE
const ELASTIC_NODE = process.env.ELASTIC_NODE || 'http://localhost:9200'; // Replace with your Elasticsearch URL
const ELASTIC_API_KEY = process.env.ELASTIC_API_KEY; // Use API Key or other auth methods
const INDEX_PATTERN = 'my_paywatch_logs-*'; // Adjust to your actual index name or pattern

// --- Create Elasticsearch Client ---
// Use API Key Authentication (Recommended)
// Ensure you have the API Key ID and Value
let client: Client;
if (ELASTIC_API_KEY) {
   client = new Client({
    node: ELASTIC_NODE,
    auth: {
      apiKey: ELASTIC_API_KEY
    },
    // Add other configurations like TLS/SSL if needed
    // ssl: { rejectUnauthorized: false } // Use cautiously for development only
  });
} else {
  // Add other authentication methods if needed (e.g., username/password)
  console.warn('Elasticsearch API Key not provided. Trying anonymous connection.');
   client = new Client({ node: ELASTIC_NODE });
}


// --- Build the Query ---
async function fetchPaywatchLogs() {
  try {
    console.log(`Querying index pattern: ${INDEX_PATTERN}`);

    const requestBody: estypes.SearchRequest = {
      index: INDEX_PATTERN,
      size: 100, // How many results per page (Kibana default is often 500, adjust as needed)
                 // NOTE: The screenshot shows >127k results. You'll need pagination (from/size or search_after)
                 // to retrieve all of them reliably. This example gets the first 100.
      sort: [
        { '@timestamp': { order: 'desc' } } // Default Kibana sort
      ],
      query: {
        bool: {
          // KQL "and" clauses go into "must" or "filter"
          must: [
            // KQL free text search: "RetrieveUserInfo"
            // This searches default fields. Adjust 'query' or field list if needed.
            {
              query_string: {
                query: '"RetrieveUserInfo"', // Keep quotes if it's a phrase search
                // default_field: '*' // Or specify fields like ['message', 'payload.somefield']
              }
            }
          ],
          filter: [
            // KQL field query: action:response
            // Use 'term' for exact keyword matches (common for fields like 'action')
            // Use 'match' if the 'action' field is analyzed text
            { term: { 'action.keyword': 'response' } }, // Adjust field name if needed (e.g., 'action' or 'action.keyword')

            // Time Range Filter
            {
              range: {
                '@timestamp': { // Use your actual time field name
                  gte: '2025-02-01T00:00:00.000Z', // Use ISO 8601 format. 'Z' denotes UTC. Adjust timezone if necessary.
                  lte: '2025-02-05T00:00:00.000Z', // 'lte' for less than or equal to
                  format: 'strict_date_optional_time' // Specify format if needed
                }
              }
            }
          ]
        }
      },
      // _source: ['aid', 'payload', '@timestamp'] // Optionally specify only needed fields for performance
    };

    console.log("Sending query to Elasticsearch:", JSON.stringify(requestBody, null, 2));

    const response: estypes.SearchResponse<PaywatchLog> = await client.search(requestBody);

    console.log(`Total Hits: ${response.hits.total ? (response.hits.total as any).value : 0}`);

    // Extract the documents
    const documents = response.hits.hits.map(hit => ({
      id: hit._id,
      score: hit._score,
      ...hit._source // Spread the document source fields
    }));

    console.log(`Retrieved ${documents.length} documents:`);
    documents.forEach((doc, index) => {
      console.log(`--- Document ${index + 1} (ID: ${doc.id}) ---`);
      console.log(JSON.stringify(doc, null, 2));
    });

    return documents;

  } catch (error) {
    console.error('Error querying Elasticsearch:', error instanceof Error ? error.message : error);
     if (error && typeof error === 'object' && 'meta' in error) {
       console.error('Elasticsearch client error details:', JSON.stringify((error as any).meta?.body || error, null, 2));
     }
    return [];
  }
}

// --- Run the query ---
fetchPaywatchLogs();
