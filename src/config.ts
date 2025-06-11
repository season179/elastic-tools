import * as dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

// Check for required environment variables
if (!process.env.ELASTIC_CLOUD_ID || !process.env.ELASTIC_API_KEY || !process.env.ELASTIC_INDEX_PATTERN) {
    throw new Error(
        "Missing required environment variables: ELASTIC_CLOUD_ID, ELASTIC_API_KEY, and/or ELASTIC_INDEX_PATTERN"
    );
}

export const config = {
    elastic: {
        cloudId: process.env.ELASTIC_CLOUD_ID as string,
        apiKey: process.env.ELASTIC_API_KEY as string,
        indexPattern: process.env.ELASTIC_INDEX_PATTERN as string,
    },
    queries: {
        retrieveUserInfo: {
            module: "RetrieveUserInfo",
            action: "response"
        },
        bukopin: {
            payloadSearch: "https://103.211.83.99:8310/api/paywatch/inquiry",
            action: "response",
            module: "Request.post",
            type: "PayoutService"
        }
    }
    // Add other configuration sections if needed, e.g., database
    // prisma: { ... }
};
