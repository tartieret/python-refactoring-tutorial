import psycopg2
import requests
import os
from datetime import datetime, timedelta


def run_etl():
    """Run the ETL process.

    This code connects to a PostgreSQL database, retrieves data from the last hour,
    transforms the results and sends the data to a third-party HTTP server.

    The 3rd party server requires the data to be split by category of product, so we have to send
    the data in batches.

    """
    # Connect to the database
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )
    cursor = conn.cursor()

    # Calculate timestamp for 1 hour ago
    one_hour_ago = datetime.now() - timedelta(hours=1)

    # Execute a single query to get all data from the last hour
    cursor.execute(
        """SELECT id, user_id, item, quantity, price, category_id, timestamp 
           FROM purchases 
           WHERE timestamp >= %s""",
        (one_hour_ago,),
    )
    results = cursor.fetchall()

    # Group results by category_id
    categorized_data = {}
    for row in results:
        category_id = row[5]  # category_id is at index 5

        if category_id not in categorized_data:
            categorized_data[category_id] = {"category_id": category_id, "data": []}

        # Transform the data
        categorized_data[category_id]["data"].append(
            {
                "user_id": row[1],
                "item_name": row[2].upper(),
                "total_spent": row[3] * row[4],
                "timestamp": row[6].isoformat() if row[6] else None,
            }
        )

    # Send each category batch to the API
    for category_id, transformed in categorized_data.items():
        response = requests.post(
            "https://api.example.com/receive",
            json=transformed,
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + os.getenv("API_TOKEN"),
            },
        )
        print(f"Category {category_id} - Status Code: {response.status_code}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    run_etl()
