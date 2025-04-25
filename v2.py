"""
ETL process implementation with error handling and logging.

This version improves upon v1 by adding proper error handling and logging.
"""

import os
import logging
import sys
import psycopg2
import requests


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl_process")


def run_etl() -> None:
    """Run the ETL process.

    This code connects to a PostgreSQL database, performs 5 queries,
    transforms the results and sends the data to a third-party HTTP server.

    Adds error handling and logging compared to v1.
    """
    conn = None
    cursor = None

    try:
        # Connect to database
        logger.info("Connecting to database")
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
        )
        cursor = conn.cursor()

        # Process each batch separately
        for i in range(5):
            try:
                # Extract data
                logger.info(f"Executing query for category_id = {i}")
                cursor.execute(
                    "SELECT id, user_id, item, quantity, price FROM purchases WHERE category_id = %s",
                    (i,),
                )
                results = cursor.fetchall()
                logger.info(f"Retrieved {len(results)} rows for category_id = {i}")

                # Skip if no results
                if not results:
                    logger.warning(f"No data retrieved for category_id = {i}")
                    continue

                # Transform data
                transformed = {"category_id": i, "data": []}
                for row in results:
                    transformed["data"].append(
                        {
                            "user_id": row[1],
                            "item_name": row[2].upper(),
                            "total_spent": row[3] * row[4],
                        }
                    )

                # Load data to API
                logger.info(
                    f"Sending batch for category_id = {i} with {len(transformed['data'])} records to API"
                )
                try:
                    response = requests.post(
                        "https://api.example.com/receive",
                        json=transformed,
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": "Bearer " + os.getenv("API_TOKEN"),
                        },
                        timeout=30,  # Add timeout to prevent hanging
                    )

                    if response.status_code >= 200 and response.status_code < 300:
                        logger.info(
                            f"Batch {i} - API request successful: Status Code {response.status_code}"
                        )
                    else:
                        logger.error(
                            f"Batch {i} - API request failed: Status Code {response.status_code}, Response: {response.text}"
                        )
                except requests.RequestException as e:
                    logger.error(f"Batch {i} - API request error: {e}")
            except psycopg2.Error as e:
                logger.error(f"Database error during query for category_id = {i}: {e}")
                # Continue with other categories even if one fails
                continue

    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        # Clean up resources
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("ETL process completed")


if __name__ == "__main__":
    logger.info("Starting ETL process")
    run_etl()
