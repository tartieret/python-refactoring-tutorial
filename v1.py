import psycopg2
import requests
import os


def run_etl():
    """Run the ETL process.

    This code connects to a PostgreSQL database, perform 5 queries,
    transform the results and send the data to a third-party HTTP server.
    """
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )
    cursor = conn.cursor()

    for i in range(5):
        cursor.execute(
            "SELECT id, user_id, item, quantity, price FROM purchases WHERE category_id = %s",
            (i,),
        )
        results = cursor.fetchall()
        transformed = {"category_id": i, "data": []}
        for row in results:
            transformed["data"].append(
                {
                    "user_id": row[1],
                    "item_name": row[2].upper(),
                    "total_spent": row[3] * row[4],
                }
            )

        # Push each batch of data to the HTTP endpoint separately
        # we assume the 3rd party server required token authentication
        response = requests.post(
            "https://api.example.com/receive",
            json=transformed,
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + os.getenv("API_TOKEN"),
            },
        )
        print(f"Batch {i} - Status Code: {response.status_code}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    run_etl()
