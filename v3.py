"""
ETL process implementation with separated responsibilities.

This module implements an ETL (Extract-Transform-Load) process with separated responsibilities.
It improves upon previous versions by splitting the code into separate functions with clear
responsibilities, making it more maintainable and testable.

Each function handles a specific part of the ETL process:
- run_query: Extracts data from the database for a specific category
- transform_data: Transforms the raw data into the required format
- load_data: Loads the transformed data to the API endpoint
- run_etl: Orchestrates the entire ETL process
"""
import os
import logging
import sys
from typing import List, Dict, Any, Tuple

import psycopg2
import requests


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("etl_process")


def run_query(param: int) -> List[Tuple]:
    """Run a query and return the results.
    
    Args:
        param (int): The category_id parameter to filter purchases.
        
    Returns:
        List[Tuple]: A list of tuples containing the query results.
    """
    logger.info(f"Executing query for category_id = {param}")
    
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            host=os.getenv('PG_HOST'),
            database=os.getenv('PG_DB'),
            user=os.getenv('PG_USER'),
            password=os.getenv('PG_PASSWORD')
        )
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, user_id, item, quantity, price FROM purchases WHERE category_id = %s", 
            (param,)
        )
        results = cursor.fetchall()
        logger.info(f"Retrieved {len(results)} rows for category_id = {param}")
        return results
    except psycopg2.Error as e:
        logger.error(f"Database error during query for category_id = {param}: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def transform_data(category_id: int, records: List[Tuple]) -> Dict[str, Any]:
    """Transform the raw data into the required format.
    
    Args:
        category_id (int): The category ID for this batch of data.
        records (List[Tuple]): A list of tuples containing the raw data from the database.
        
    Returns:
        Dict[str, Any]: A dictionary with categoryId and data array containing the transformed records.
    """
    logger.info(f"Transforming {len(records)} records for category_id = {category_id}")
    
    transformed = {
        "categoryId": category_id,
        "data": []
    }
    
    for row in records:
        transformed["data"].append({
            "userId": row[1],
            "itemName": row[2].upper(),
            "totalSpent": row[3] * row[4]
        })
    
    logger.info(f"Transformation complete, produced {len(transformed['data'])} records for category_id = {category_id}")
    return transformed


def load_data(data: Dict[str, Any]) -> bool:
    """Load the transformed data to the API endpoint.
    
    Args:
        data (Dict[str, Any]): A dictionary containing the transformed data with categoryId and data array.
        
    Returns:
        bool: True if the data was successfully loaded, False otherwise.
    """
    if not data or not data.get("data"):
        logger.warning("No data to load")
        return False
    
    category_id = data.get("categoryId")
    record_count = len(data["data"])
    logger.info(f"Sending batch for category_id = {category_id} with {record_count} records to API")
    
    try:
        response = requests.post(
            "https://api.example.com/receive",
            json=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + os.getenv('API_TOKEN')
            },
            timeout=30
        )
        
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Batch {category_id} - API request successful: Status Code {response.status_code}")
            return True
        else:
            logger.error(f"Batch {category_id} - API request failed: Status Code {response.status_code}, Response: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Batch {category_id} - API request error: {e}")
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
            records = run_query(i)
            
            # Skip if no data was found
            if not records:
                logger.warning(f"No data retrieved for category_id = {i}, skipping")
                continue
                
            # Transform the data
            transformed = transform_data(i, records)
            
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
