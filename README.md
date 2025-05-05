# Elasticsearch Data Puller & Processor

This script connects to an Elasticsearch cluster, retrieves log documents based on a specified date range and query criteria, processes the payload, and inserts relevant data into a PostgreSQL database using Prisma.

## Prerequisites

*   Node.js (v16 or later recommended)
*   npm (usually comes with Node.js)
*   Access to an Elasticsearch cluster (Cloud ID and API Key)
*   Access to a PostgreSQL database

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd elastic-pull
    ```

2.  **Install dependencies:**
    ```bash
    npm install
    ```

3.  **Configure Environment Variables:**
    Create a `.env` file in the root directory of the project and add your Elasticsearch connection details, the index pattern, and your database connection URL.

    *   **Example:**
        ```dotenv
        # .env
        ELASTIC_CLOUD_ID="YOUR_CLOUD_ID"
        ELASTIC_API_KEY="YOUR_API_KEY"
        ELASTIC_INDEX_PATTERN="your-index-pattern-*" # e.g., filebeat-*, logs-*, etc.
        DATABASE_URL="postgresql://USER:PASSWORD@HOST:PORT/DATABASE"
        ```

    *Make sure the `.env` file is added to your `.gitignore` if it's not already.*

4.  **Database Setup (Prisma):**
    *   Ensure your PostgreSQL database schema matches the model defined in `prisma/schema.prisma` (currently `UserLogin`).
    *   If this is the first time or the schema changed, generate the Prisma client:
        ```bash
        npx prisma generate
        ```
    *   Apply database migrations if necessary (refer to Prisma documentation).

5.  **Modify Query (Optional):**
    Adjust the Elasticsearch query conditions (like `module` and `action`) within `src/services/elasticsearch.ts` if needed.

## Usage

Run the script using `npm start`, providing the required start and end dates as command-line arguments. Use the `--` separator to pass arguments to the script.

```bash
npm start -- --startDate <YYYY-MM-DD> --endDate <YYYY-MM-DD>
```

*   `--startDate` (`-s`): The beginning date for the data pull (inclusive), in `YYYY-MM-DD` format.
*   `--endDate` (`-e`): The ending date for the data pull (exclusive), in `YYYY-MM-DD` format.

**Example:**

To retrieve data from January 15th, 2025 up to (but not including) January 17th, 2025:

```bash
npm start -- --startDate 2025-01-15 --endDate 2025-01-17
```

This command will fetch data for January 15th and January 16th.

The script will print progress information, including the total number of documents fetched from Elasticsearch, the number of records processed, and the number of records inserted or skipped (due to duplicates) in the PostgreSQL database.
