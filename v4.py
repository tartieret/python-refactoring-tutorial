"""
ETL process implementation with stronger isolation of concerns and better typing.

This module implements an ETL (Extract-Transform-Load) process with improved type safety
and clearer boundaries between components using dataclasses.

Each function handles a specific part of the ETL process:
- run_query: Extracts data from the database for a specific category
- transform_data: Transforms the raw data into the required format
- load_data: Loads the transformed data to the API endpoint
- run_etl: Orchestrates the entire ETL process
"""

import os
import logging
import sys
from dataclasses import dataclass, asdict
from typing import List

import psycopg2
import requests


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl_process")


@dataclass
class Purchase:
    """Represents a purchase record from the database.

    Attributes:
        id (int): The unique identifier of the purchase.
        user_id (int): The identifier of the user who made the purchase.
        item (str): The name of the purchased item.
        quantity (int): The number of items purchased.
        price (float): The price per item.
    """

    id: int
    user_id: int
    item: str
    quantity: int
    price: float

    @property
    def total_spent(self) -> float:
        """Calculate the total amount spent on this purchase.

        Returns:
            float: The product of quantity and price.
        """
        return self.quantity * self.price


def run_query(param: int) -> List[Purchase]:
    """Run a query and return the results as Purchase objects.

    Args:
        param (int): The category_id parameter to filter purchases.

    Returns:
        List[Purchase]: A list of Purchase objects containing the query results.
    """
    logger.info(f"Executing query for category_id = {param}")

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, user_id, item, quantity, price FROM purchases WHERE category_id = %s",
            (param,),
        )
        results = cursor.fetchall()
        logger.info(f"Retrieved {len(results)} rows for category_id = {param}")

        # Convert database rows to Purchase objects
        purchases = [
            Purchase(
                id=row[0], user_id=row[1], item=row[2], quantity=row[3], price=row[4]
            )
            for row in results
        ]

        return purchases
    except psycopg2.Error as e:
        logger.error(f"Database error during query for category_id = {param}: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@dataclass
class APIRecord:
    """Represents a single record to be sent to the API.

    Attributes:
        user_id (int): The identifier of the user who made the purchase.
        item_name (str): The uppercase name of the purchased item.
        total_spent (float): The total amount spent on this purchase.
    """

    user_id: int
    item_name: str
    total_spent: float


@dataclass
class APIData:
    """Represents the transformed data structure to be sent to the API.

    Attributes:
        category_id (int): The category ID for this batch of data.
        data (List[APIRecord]): The list of transformed purchase records.
    """

    category_id: int
    data: List[APIRecord]


def transform_data(category_id: int, records: List[Purchase]) -> APIData:
    """Transform the raw data into the required format.

    Args:
        category_id (int): The category ID for this batch of data.
        records (List[Purchase]): A list of Purchase objects containing the raw data.

    Returns:
        APIData: An object with category_id and data array containing the transformed records.
    """
    logger.info(f"Transforming {len(records)} records for category_id = {category_id}")

    api_records = [
        APIRecord(
            user_id=purchase.user_id,
            item_name=purchase.item.upper(),
            total_spent=purchase.total_spent,
        )
        for purchase in records
    ]

    transformed = APIData(category_id=category_id, data=api_records)

    logger.info(
        f"Transformation complete, produced {len(transformed.data)} records for category_id = {category_id}"
    )
    return transformed


def load_data(data: APIData) -> bool:
    """Load the transformed data to the API endpoint.

    Args:
        data (APIData): An object containing the transformed data with category_id and data array.

    Returns:
        bool: True if the data was successfully loaded, False otherwise.
    """
    if not data.data:
        logger.warning("No data to load")
        return False

    logger.info(
        f"Sending batch for category_id = {data.category_id} with {len(data.data)} records to API"
    )

    try:
        response = requests.post(
            "https://api.example.com/receive",
            json=asdict(data),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + os.getenv("API_TOKEN"),
            },
            timeout=30,
        )

        if response.status_code >= 200 and response.status_code < 300:
            logger.info(
                f"Batch {data.category_id} - API request successful: Status Code {response.status_code}"
            )
            return True
        else:
            logger.error(
                f"Batch {data.category_id} - API request failed: Status Code {response.status_code}, Response: {response.text}"
            )
            return False
    except requests.RequestException as e:
        logger.error(f"Batch {data.category_id} - API request error: {e}")
        return False


def run_etl() -> bool:
    """Run the complete ETL process.

    Extracts data from the database for each category, transforms it, and loads it to the API.
    Each category is processed and sent as a separate batch.

    Returns:
        bool: True if the ETL process completed successfully, False otherwise.

    Raises:
        Exception: Catches and logs any unexpected exceptions during processing.
    """
    logger.info("Starting ETL process")

    try:
        # Process each batch separately
        success = True
        for i in range(5):
            # Extract data for this category
            purchases = run_query(i)

            # Skip if no data was found
            if not purchases:
                logger.warning(f"No data retrieved for category_id = {i}, skipping")
                continue

            # Transform the data
            transformed = transform_data(i, purchases)

            # Load the data
            batch_success = load_data(transformed)

            # Track overall success
            if not batch_success:
                success = False

        logger.info("ETL process completed")
        return success
    except Exception as e:
        logger.error(f"Unexpected error in ETL process: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    run_etl()
