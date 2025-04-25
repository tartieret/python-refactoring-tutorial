# Writing Quality Python Code: An ETL Process Example

This tutorial demonstrates a pragmatic approach to writing quality Python code through an iterative improvement process. We'll focus on three key aspects:

1. **Readability**: Making code easy to understand for humans
2. **Testability**: Ensuring that the code is organized in a way that makes it easy to write tests
3. **Typing**: Thinking about types, as a way to document the code but also improve its performance

## Why This Matters

By code quality, we generally mean the ability of the code to be:

- Easy to understand for humans
- Easy to test
- Easy to maintain and evolve

This is often mixed up with using advanced language features or following complex patterns. Aiming at "beautiful code", many developers tend to prematurely optimize or over-engineer their code, which can lead to code that is paradoxically hard to understand and maintain.

Over the last few years working on data platforms, I have seen simple pieces of code running unchanged for several years, even if they did not look great. I have also seen code that was written with advanced design patterns, and that had to be completely refactored the next year when an evolution of the business requirements showed that the anticipated "future" was not aligned with the actual "new" needs.

In this tutorial, I'll focus on a practical example directly inspired from recent code reviews in my team. I'll show you how your mindset and work process can directly impact the quality of your code. We'll also discuss the trade-offs between time spent and code structure.

## The Example: An ETL Process

We'll build a program that implements a simple Extract-Transform-Load (ETL) process, taking the example of an e-commerce application that needs to send data to a third-party HTTP server (for instance for business analytics):

- **Extract**: Retrieve all purchases for the last hour
- **Transform**: Process and modify the extracted data to match the third-party HTTP server's requirements
- **Load**: Send the transformed data to a third-party HTTP server. Unfortunately, the third-party HTTP API requires the data to be split by category of product, so we have to send the data in batches. We don't have control on this server, so we can't change that.

## Tutorial Structure

This tutorial follows an iterative improvement approach. We'll start with a basic implementation and progressively enhance it:

- **Version 1**: Basic implementation with minimal structure
- **Version 2**: Adding Error Handling and Logging
- **Version 3**: Split the responsibilities into different functions

Each version will be saved in its own Python module (`v1.py`, `v2.py`, etc.) so you can see the progression of improvements. In a real project, the code will be broken into different files and modules.

# v1: Simple Implementation

Our first version is a straightforward implementation of the ETL process in a single function. It connects to a PostgreSQL database, execute a query to retrieve data from the last hour, transforms the results, and sends the data to the third-party HTTP server.

```python
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
           WHERE timestamp >= %s
           ORDER BY category_id""",
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
```

## Analysis of v1

This implementation isn't necessarily "bad" code. It efficiently retrieves data from the last hour using a timestamp column and groups it by category before sending it to the API. This approach is more efficient than running multiple queries as it minimizes database round trips.

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
                    "category_id": i,
                    "data": []
                }
                for row in results:
                    transformed["data"].append({
                        "user_id": row[1],
                        "item_name": row[2].upper(),
                        "total_spent": row[3] * row[4]
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

We can see that once we bring in "real life" considerations with error handling and different possible code paths, the code turns into spaghetti soup and becomes hard to understand. If you were not convinced, the point raised in the previous section about testability is even more relevant: it's cumbersome to write tests for all the possible paths.

When I see this type of code, it typically means that the developer considered testing as an afterthought, not a priority. If you try to write tests "as you go" (even if you don't follow a strict Test-Driven-Development approach), you'll tend to proceed in more elementary steps, which will lead to a more structured and maintainable codebase. This is also transparent in the git history, and typically differentiates seasoned developers, who are in control of their process, from less experienced ones, who are only aiming at finding the solution.

## v3: Split the responsibilities into different functions

Let's take a step back, restart from scratch and follow a more structured approach.

First, we'll want to write a function that runs a query and returns the results. We can easily write tests for that, making sure that the SQL query is correct.

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
        "category_id": category_id,
        "data": []
    }
    for row in records:
        transformed["data"].append({
            "user_id": row[2],
            "item_name": row[2].upper(),
            "total_spent": row[3] * row[4]
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
        transformed = transform_data(i, data)
        load_data(transformed)
```

This main function gives a clear picture of what the ETL process does, and it's much easier to understand before diving into the details.

Note that this new version is not much longer than the previous one, and wouldn't take more time to write. Following a different process while writing the code can greatly improve its quality. It provides several benefits:

- you can catch errors earlier
- you can write tests more easily
- your reviewer will have to spend less time understanding the code, and the likeliness of missing a defect is reduced

## v4: Stronger isolation of concerns with better typing

In real life, the code from v3 may be fine for a while. However, as the system evolves, your database schema may change, and the query will have to be updated. The same goes for the API endpoint, which may change over time.

We can observe that the `transform_data` function makes an assumption on the way the data is structured, to be able to calculate the total amount spent by a user by multiplying the number of purchased items with their unit price, as shown in the snippet below:

```python
for row in records:
    transformed["data"].append({
        "user_id": row[2],
        "item_name": row[2].upper(),
        "total_spent": row[3] * row[4]
    })
```

This means that if I want to check that this code is correct, I have to go back to the `run_query` function, and even within this function review the SQL query itself. In the type of applications that I have been working on lately, this SQL query may be extremely complex, making this kind of review time-consuming and error-prone. In addition, if the database schema changes, the query will have to be updated, and the `transform_data` function will have to be updated as well, which may introduce new errors.

This could have been avoided from the start by defining a better "contract" between each part of the code.

Let's define a type for the data that is passed between the functions:

```python
@dataclass
class Purchase:
    id: int
    user_id: int
    item: str
    quantity: int
    price: float
```

This defines a new data structure with a fixed number of properties and typing information, making it clear what data is expected and what is returned.

Now, let's update `run_query` to use this type:

```python
def run_query(param: int) -> List[Purchase]:
    """Run a query and return the results."""
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
        cursor.execute("SELECT id, user_id, item, quantity, price FROM purchases WHERE category_id = %s", (param,))
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return [Purchase(id=row[0], user_id=row[1], item=row[2], quantity=row[3], price=row[4]) for row in results]
    except Exception as e:
        logger.error(f"Database error during query for category_id = {param}: {e}")
        return []
```

We can now see that the return statement of `run_query` is now clear and self-documenting. If the SQL query is modified in the future, we only have to update the function and may not have to change the `Purchase` type.

Let's update `transform_data` to use this type:

```python
def transform_data(category_id: int, records: List[Purchase]) -> Dict[str, Any]:
    """Transform the data."""
    transformed = {
        "category_id": category_id,
        "data": []
    }
    
    for row in records:
        transformed["data"].append({
            "user_id": row.user_id,
            "item_name": row.item.upper(),
            "total_spent": row.quantity * row.price
        })
    
    return transformed
```

The calculation of `totalSpent` is now self-documenting and we can see that it's the product of `quantity` and `price`. Purists may argue that this function is still doing too much. The advantage of using a dataclass is that we can consider moving the calculation of `totalSpent` to a separate function, which would make the code more maintainable, like this:

```python
@dataclass
class Purchase:
    id: int
    user_id: int
    item: str
    quantity: int
    price: float
    
    @property
    def total_spent(self) -> float:
        """Calculate the total amount spent on this purchase."""
        return self.quantity * self.price
```

And the final version of `transform_data` would be:

```python
def transform_data(category_id: int, records: List[Purchase]) -> Dict[str, Any]:
    """Transform the data."""
    transformed = {
        "category_id": category_id,
        "data": []
    }
    
    for row in records:
        transformed["data"].append({
            "user_id": row.user_id,
            "item_name": row.item.upper(),
            "total_spent": row.total_spent
        })
    
    return transformed
```

The attentive reader will notice that we have the same dependency and readability problem between the `transform_data` function and the `load_data` one.

Let's fix this by creating a new dataclass for the API data:

```python

@dataclass
class APIRecord:
    """Represents a single record to be sent to the API."""
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
```

Now, let's update `transform_data` and `load_data` to use this new dataclass:

```python
from dataclasses import asdict

def transform_data(category_id: int, records: List[Purchase]) -> APIData:
    """Transform the data."""
    return APIData(
        category_id=category_id,
        data=[APIRecord(
            user_id=row.user_id,
            item_name=row.item.upper(),
            total_spent=row.total_spent
        ) for row in records]
    )

def load_data(payload: APIData) -> None:
    """Load the data."""
    response = requests.post(
        "https://api.example.com/receive",
        json=asdict(payload),
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

The code is now more maintainable and testable, and the contract between the functions is clearer. We can feel happy, we have written "nice code".

It's important to pause here and to consider the benefit/cost ratio of this kind of refactoring. This code is better organized and more maintainable, but it's also 40% longer than v3. For a single-use script, you would probably be wasting time here, and it's important to keep this in mind. However, in a production grade codebase, with a focus on testing, a large part of the coding part will actually be spent on writing tests, so this better organization of the code may actually lead to a net time saving, compared to reaching the same level of test coverage with v2.

However, there is no free lunch and if you consider actually running this code, v4 will run at a much higher cost if it processes a non-negligible amount of data. Can you detect the problem?

## Memory consideration

When going through this kind of refactoring exercise, it's very easy to try to write "beautiful code". However, that's never the goal, as software developers we are paid to solve problems and not to create pieces of art.
