"""
ETL process implementation with error handling, logging, and retry logic.

This version improves upon v1 by adding proper error handling, logging, and retry logic for API calls.
"""

import os
import logging
import sys
import psycopg2
import requests
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl_process")


def run_etl() -> None:
    """Run the ETL process.

    This code connects to a PostgreSQL database, retrieves data from the last hour,
    transforms the results and sends the data to a third-party HTTP server.

    The 3rd party server requires the data to be split by category of product, so we have to send
    the data in batches.

    Adds error handling, logging, and retry logic compared to v1.
    """
    logger.info("Starting ETL process")

    conn = None
    cursor = None

    # Setup retry strategy for API calls
    retry_strategy = Retry(
        total=3,  # Maximum number of retries
        backoff_factor=1,  # Time factor between retries
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

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

        # Calculate timestamp for 1 hour ago
        one_hour_ago = datetime.now() - timedelta(hours=1)
        logger.info(f"Retrieving data since {one_hour_ago.isoformat()}")

        try:
            # Execute a single query to get all data from the last hour
            cursor.execute(
                """SELECT id, user_id, item, quantity, price, category_id, timestamp 
                FROM purchases 
                WHERE timestamp >= %s""",
                (one_hour_ago,),
            )
            results = cursor.fetchall()
            logger.info(f"Retrieved {len(results)} total rows")

            # Skip if no results
            if not results:
                logger.warning("No data retrieved for the last hour")
                return

            # Group results by category_id
            categorized_data = {}
            for row in results:
                category_id = row[5]  # category_id is at index 5

                if category_id not in categorized_data:
                    categorized_data[category_id] = {
                        "category_id": category_id,
                        "data": [],
                    }

                # Transform the data
                categorized_data[category_id]["data"].append(
                    {
                        "user_id": row[1],
                        "item_name": row[2].upper(),
                        "total_spent": row[3] * row[4],
                        "timestamp": row[6].isoformat() if row[6] else None,
                    }
                )

            logger.info(f"Data grouped into {len(categorized_data)} categories")

            # Send each category batch to the API
            for category_id, transformed in categorized_data.items():
                try:
                    logger.info(
                        f"Sending batch for category_id = {category_id} with {len(transformed['data'])} records to API"
                    )
                    response = session.post(
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
                            f"Category {category_id} - API request successful: Status Code {response.status_code}"
                        )
                    else:
                        logger.error(
                            f"Category {category_id} - API request failed: Status Code {response.status_code}, Response: {response.text}"
                        )
                except requests.RequestException as e:
                    logger.error(f"Category {category_id} - API request error: {e}")

        except psycopg2.Error as e:
            logger.error(f"Database error during query execution: {e}")

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
    run_etl()
