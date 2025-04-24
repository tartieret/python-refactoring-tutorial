# Writing Quality Python Code: An ETL Process Example

This tutorial demonstrates how to write high-quality Python code through an iterative improvement process. We'll focus on three key aspects of quality code:

1. **Readability**: Making code easy to understand for humans
2. **Type Annotations**: Using Python's typing system to make code more robust
3. **Testing**: Ensuring code correctness through automated tests

## The Example: An ETL Process

We'll build a program that implements a simple Extract-Transform-Load (ETL) process:

- **Extract**: Execute SQL queries with different parameters to retrieve purchases from a database
- **Transform**: Process and modify the extracted data
- **Load**: Send the transformed data to a third-party HTTP server

## Tutorial Structure

This tutorial follows an iterative improvement approach. We'll start with a basic implementation and progressively enhance it:

- **Version 1**: Basic implementation with minimal structure
- **Version 2**: Adding Error Handling and Logging
- **Version 3**: Split the responsibilities into different functions

Each version will be saved in its own Python file (`v1.py`, `v2.py`, etc.) so you can see the progression of improvements.

## Why This Matters

Writing quality code isn't just about making things work—it's about creating maintainable, robust software that:

- Is easy for others (and your future self) to understand
- Catches errors early through type checking
- Can be confidently modified without breaking existing functionality
- Is well-tested to ensure correctness

Let's begin our journey to better Python code!

# v1: Simple Implementation

Our first version is a straightforward implementation of the ETL process in a single function. It connects to a PostgreSQL database, executes 5 queries with different parameters, transforms the results, and sends the data to a third-party HTTP server.

Note for the nitpickers: yes we could retrieve all the data in one query, but that's not the point here. Let's consider a case for which we want to retrieve the data in batches.

```python
def run_etl():
    """Run the ETL process. 
    
    This code connects to a PostgreSQL database, perform 5 queries, 
    transform the results and send the data to a third-party HTTP server.
    """
    conn = psycopg2.connect(
        host=os.getenv('PG_HOST'),
        database=os.getenv('PG_DB'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'))
    cursor = conn.cursor()

    for i in range(5):
        cursor.execute("SELECT id, user_id, item, quantity, price FROM purchases WHERE category_id = ?", (i,))
        results = cursor.fetchall()
        transformed = {
            "categoryId": i,
            "data": []
        }
        for row in results:
            transformed["data"].append({
                "userId": row[1],
                "itemName": row[2].upper(),
                "totalSpent": row[3] * row[4]
            })
        
        # Push each batch of data to the HTTP endpoint separately
        # we assume the 3rd party server required token authentication
        response = requests.post("https://api.example.com/receive",
                                json=transformed,
                                headers={
                                    "Content-Type": "application/json",
                                    "Authorization": "Bearer " + os.getenv('API_TOKEN')
                                })
        print(f"Batch {i} - Status Code: {response.status_code}")

    cursor.close()
    conn.close()
```

## Analysis of v1

This implementation isn't necessarily "bad" code. In fact, it's the kind of thing you might write as a single-use tool—quick and dirty, getting the job done without much ceremony.

However, in the context of a larger project, this approach isn't ideal for several reasons:

1. **Testing Challenges**: It would be difficult to write tests for this code as you'd need to:
   - Generate test data in a test database
   - Mock the calls to the third-party API

   These testing difficulties are a red flag, typically indicating that the code isn't well organized.

2. **Single Responsibility Principle**: The `run_etl()` function does everything—connecting to the database, querying, transforming data, and sending it to an API. While this piece of code is short enough that it's not too hard to read and review, this approach doesn't scale well as complexity increases.

3. **Configuration**: Environment variables are accessed directly in the function, making it harder to configure in different environments.

# v2: Adding Error Handling and Logging

The previous version didn't include any error handling for the sake of simplicity. Now let's imagine you are integrating this code in a production application, running on a server for instance as a periodic job. You'll want to implement additional error handling and logging to make the code more robust and easier to monitor.

Here's a snippet of the modified code:

```python
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
            host=os.getenv('PG_HOST'),
            database=os.getenv('PG_DB'),
            user=os.getenv('PG_USER'),
            password=os.getenv('PG_PASSWORD')
        )
        cursor = conn.cursor()
        
        # Process each batch separately
        for i in range(5):
            try:
                # Extract data
                logger.info(f"Executing query for category_id = {i}")
                cursor.execute(
                    "SELECT id, user_id, item, quantity, price FROM purchases WHERE category_id = %s", 
                    (i,)
                )
                results = cursor.fetchall()
                logger.info(f"Retrieved {len(results)} rows for category_id = {i}")
                
                # Skip if no results
                if not results:
                    logger.warning(f"No data retrieved for category_id = {i}")
                    continue
                
                # Transform data
                transformed = {
                    "categoryId": i,
                    "data": []
                }
                for row in results:
                    transformed["data"].append({
                        "userId": row[1],
                        "itemName": row[2].upper(),
                        "totalSpent": row[3] * row[4]
                    })
                
                # Load data to API
                logger.info(f"Sending batch for category_id = {i} with {len(transformed['data'])} records to API")
                try:
                    response = requests.post(
                        "https://api.example.com/receive",
                        json=transformed,
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": "Bearer " + os.getenv('API_TOKEN')
                        },
                        timeout=30  # Add timeout to prevent hanging
                    )
                    
                    if response.status_code >= 200 and response.status_code < 300:
                        logger.info(f"Batch {i} - API request successful: Status Code {response.status_code}")
                    else:
                        logger.error(f"Batch {i} - API request failed: Status Code {response.status_code}, Response: {response.text}")
                except requests.RequestException as e:
                    logger.error(f"Batch {i} - API request error: {e}")
            except psycopg2.Error as e:
                logger.error(f"Database error during query for category_id = {i}: {e}")
                # Continue with other categories even if one fails
                continue
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        # Clean up resources
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("ETL process completed")
```

We can see that once we bring in "real life" consideration with error handling and different possible code paths, the code turns into spaghetti soup and becomes hard to understand. If you were not convinced, the point raised in the previous section about testability is even more relevant: it's cumbersome to write tests for all the possible paths.

When I see this type of code, it typically means that the developer consider testing as an afterthought, not a priority. If you try to write tests "as you go" (even if you don't follow a strict Test-Driven-Development approach), you'll tend to proceed in more elementary steps, which will lead to a more structured and maintainable codebase.

## v3: Split the responsibilities into different functions

Let's take a step back, restart from scratch and follow a more structured approach.

First, we'll want to write a function that run a query and return the results. We can easily write tests for that, making sure that the SQL query is correct.

```python
def run_query(param: int) -> list[tuple]:
    """Run a query and return the results."""
    conn = psycopg2.connect(
        host=os.getenv('PG_HOST'),
        database=os.getenv('PG_DB'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD')
    )
    cursor = conn.cursor()
    cursor.execute("SELECT id, user_id, item, quantity, price FROM purchases WHERE category_id = %s", (param,))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results
```

As a second step, I can now focus on the transformation of the data.

```python
def transform_data(category_id: int, records: list[tuple]) -> dict:
    """Transform the data."""
    transformed = {
        "categoryId": category_id,
        "data": []
    }
    for row in records:
        transformed["data"].append({
            "userId": row[2],
            "itemName": row[2].upper(),
            "totalSpent": row[3] * row[4]
        })
    return transformed
```

This new function is much easier to test than the previous version, and I can easily catch a logic error in the transformation process.

As a third step, I can now focus on the loading of the data.

```python
def load_data(data: dict) -> None:
    """Load the data."""
    response = requests.post(
        "https://api.example.com/receive",
        json=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + os.getenv('API_TOKEN')
        }
    )
    if response.status_code >= 200 and response.status_code < 300:
        print(f"Status Code: {response.status_code}")
    else:
        print(f"Status Code: {response.status_code}")
```

Finally, putting it all together:

```python
def run_etl():
    """Run the ETL process."""
    for i in range(5):
        data = run_query(i)
        transformed = transform_data(data)
        load_data(transformed)
```

This main function gives a clear picture of what the ETL process does, and it's much easier to understand before diving into the details.

Note that this new version is not longer than the previous one, and wouldn't take more time to write. Following a different process while writing the code can greatly improve its quality. It provides several benefits:

- you can catch errors earlier
- you can write tests more easily
- your reviewer will have to spend less time understanding the code, and the likeliness of missing a defect is reduced
