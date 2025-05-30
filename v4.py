"""
ETL process implementation with improved separation of concerns and better typing.

This version improves upon v3b by using proper data classes to create
clear boundaries between components.
"""

import os
import logging
import sys
import psycopg2
from typing import Dict, List, Any, Optional
import requests
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dataclasses import dataclass, asdict


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("etl_process")


@dataclass
class Purchase:
    """Represents a purchase record from the database."""

    id: int
    timestamp: datetime
    category_id: int
    user_id: int
    item: str
    quantity: int
    price: float

    @property
    def total_spent(self) -> float:
        """Calculate the total amount spent on this purchase."""
        return self.quantity * self.price


@dataclass
class APIRecord:
    """Represents a single record to be sent to the API."""

    user_id: int
    item_name: str
    total_spent: float
    timestamp: Optional[str] = None


@dataclass
class APIBatch:
    """Represents a batch of data to be sent to the API for a specific category."""

    category_id: int
    data: List[APIRecord]


def extract(timestamp: datetime) -> List[Purchase]:
    """Run a query to retrieve data from the database since the given timestamp.

    Args:
        timestamp: The timestamp to filter data from

    Returns:
        List of Purchase objects containing the query results

    Raises:
        psycopg2.Error: If there's an issue with the database connection or query
    """
    logger.info(f"Retrieving data since {timestamp.isoformat()}")

    with psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    ) as conn:
        with conn.cursor() as cursor:
            # Execute query
            cursor.execute(
                """SELECT id, user_id, item, quantity, price, category_id, timestamp 
                FROM purchases 
                WHERE timestamp >= %s""",
                (timestamp,),
            )
            raw_results = cursor.fetchall()
            logger.info(f"Retrieved {len(raw_results)} total rows")
            return [
                Purchase(
                    id=row[0],
                    user_id=row[1],
                    item=row[2],
                    quantity=row[3],
                    price=row[4],
                    category_id=row[5],
                    timestamp=row[6],
                )
                for row in raw_results
            ]


def transform_data(purchases: List[Purchase]) -> Dict[int, APIBatch]:
    """Transform the purchase objects into the format required by the API.

    Args:
        purchases: List of Purchase objects from the database

    Returns:
        Dictionary mapping category_id to transformed data
    """
    logger.info("Transforming data")

    # Group results by category_id
    categorized_data = {}
    for purchase in purchases:
        category_id = purchase.category_id

        if category_id not in categorized_data:
            categorized_data[category_id] = APIBatch(
                category_id=category_id,
                data=[],
            )

        # Transform the data
        categorized_data[category_id].data.append(
            APIRecord(
                user_id=purchase.user_id,
                item_name=purchase.item.upper(),
                total_spent=purchase.total_spent,
                timestamp=purchase.timestamp.isoformat()
                if purchase.timestamp
                else None,
            )
        )

    logger.info(f"Data grouped into {len(categorized_data)} categories")
    return categorized_data


def create_api_session() -> requests.Session:
    """Create and configure a requests Session with retry logic.

    Returns:
        A configured requests Session object
    """
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

    return session


def send_batch_to_api(session: requests.Session, data: APIBatch) -> bool:
    """Send a single batch of data to the API.

    Args:
        session: The requests Session to use
        data: The data to send to the API

    Returns:
        True if the request was successful, False otherwise
    """
    try:
        logger.info(
            f"Sending batch for category_id = {data.category_id} with {len(data.data)} records to API"
        )
        response = session.post(
            "https://api.example.com/receive",
            json=asdict(data),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + os.getenv("API_TOKEN"),
            },
            timeout=30,  # Add timeout to prevent hanging
        )

        if response.status_code >= 200 and response.status_code < 300:
            logger.info(
                f"Category {data.category_id} - API request successful: Status Code {response.status_code}"
            )
            return True
        else:
            logger.error(
                f"Category {data.category_id} - API request failed: Status Code {response.status_code}, Response: {response.text}"
            )
            return False
    except requests.RequestException as e:
        logger.error(f"Category {data.category_id} - API request error: {e}")
        return False


def load_data(categorized_data: Dict[int, APIBatch]) -> None:
    """Send the transformed data to the API.

    Args:
        categorized_data: Dictionary mapping category_id to transformed data
    """
    if not categorized_data:
        logger.warning("No data to load")
        return

    # Create a session with retry logic
    session = create_api_session()

    # Track success/failure counts
    success_count = 0
    failure_count = 0

    # Send each category batch to the API
    for _, transformed in categorized_data.items():
        if send_batch_to_api(session, transformed):
            success_count += 1
        else:
            failure_count += 1

    logger.info(
        f"API requests completed: {success_count} successful, {failure_count} failed"
    )


def run_etl() -> None:
    """Run the ETL process.

    This function orchestrates the ETL process by calling the individual functions
    for extracting, transforming, and loading data.
    """
    logger.info("Starting ETL process")

    one_hour_ago = datetime.now() - timedelta(hours=1)
    purchases = extract(one_hour_ago)
    categorized_data = transform_data(purchases)
    load_data(categorized_data)

    logger.info("ETL process completed successfully")


if __name__ == "__main__":
    run_etl()
