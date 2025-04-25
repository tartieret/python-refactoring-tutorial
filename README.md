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

This implementation isn't necessarily "bad" code and would be suitable for a single-use tool as a "quick and dirty" solution. In the context of a larger project, this approach isn't ideal for several reasons:

1. **Testing Challenges**: It would be difficult to write tests for this code as you'd need to:
   - Generate test data in a test database
   - Mock the calls to the third-party API

   These testing difficulties are a red flag, typically indicating that the code isn't well organized.

2. **Single Responsibility Principle**: The `run_etl()` function does everything—connecting to the database, querying, transforming data, and sending it to an API. While this piece of code is short enough that it's not too hard to read and review, this approach doesn't scale well as complexity increases.

3. **Configuration**: Environment variables are accessed directly in the function, making it harder to configure in different environments.

# v2: Adding Error Handling and Logging

The previous version was fine as a single-use tool, but if we are considering running this periodically on a server, we need to add better logging and error handling. We'll keep things simple and make the following changes:

- add more detailed logging
- add retry logic for API calls
Here's a snippet of the modified code:

```python

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
                WHERE timestamp >= %s
                ORDER BY category_id""",
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

```

We could argue that there may be better ways to handle errors, which we'll discuss later, but the point here is that bringing in "real life" considerations turns the code into spaghetti soup that is hard to understand. If you were not convinced, the point raised in the previous section about testability is even more relevant: it's cumbersome to write tests for all the possible paths.

When I see this type of code, it typically means that the developer considered testing as an afterthought, not a priority. If you try to write tests "as you go" (even if you don't follow a strict Test-Driven-Development approach), you'll tend to proceed in more elementary steps, which will lead to a more structured and maintainable codebase. This is also transparent in the git history, and typically differentiates seasoned developers, who are in control of their process, from less experienced ones, who are only aiming at finding the solution.

## v3: Split the responsibilities into different functions

Let's take a step back, restart from scratch and follow a more structured approach.

First, we'll want to write a function that runs a query and returns the results. We can easily write tests for that, making sure that the SQL query is correct.

```python
def extract(timestamp: datetime) -> List[Tuple]:
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
            results = cursor.fetchall()
            logger.info(f"Retrieved {len(results)} total rows")

            return results
```

As a second step, I can now focus on the transformation of the data.

```python
def transform_data(results: List[Tuple]) -> Dict[int, Dict[str, Any]]:
    logger.info("Transforming data")

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
    return categorized_data
```

This new function is much easier to test than the previous version, and I can easily catch a logic error in the transformation process.

As a third step, I can now focus on the loading of the data.

```python
def load_data(categorized_data: Dict[int, Dict[str, Any]]) -> None:
    """Send the transformed data to the API.

    Args:
        categorized_data: Dictionary mapping category_id to transformed data
    """
    if not categorized_data:
        logger.warning("No data to load")
        return

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
```

This function now focused on sending the data to the API, but it's still quite long and contains different phases:

- create a session with retry logic
- send each category batch to the API

This is a step forward, but we can do better by extracting the API call logic into separate functions, as such:

```python

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


def send_batch_to_api(session: requests.Session, data: Dict[str, Any]) -> bool:
    """Send a single batch of data to the API.

    Args:
        session: The requests Session to use
        data: The data to send to the API

    Returns:
        True if the request was successful, False otherwise
    """
    try:
        logger.info(
            f"Sending batch for category_id = {data['category_id']} with {len(data['data'])} records to API"
        )
        response = session.post(
            "https://api.example.com/receive",
            json=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + os.getenv("API_TOKEN"),
            },
            timeout=30, 
        )

        if response.status_code >= 200 and response.status_code < 300:
            logger.info(
                f"Category {data['category_id']} - API request successful: Status Code {response.status_code}"
            )
            return True
        else:
            logger.error(
                f"Category {data['category_id']} - API request failed: Status Code {response.status_code}, Response: {response.text}"
            )
            return False
    except requests.RequestException as e:
        logger.error(f"Category {data['category_id']} - API request error: {e}")
        return False


def load_data(categorized_data: Dict[int, Dict[str, Any]]) -> None:
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
```

Finally, putting it all together:

```python
def run_etl() -> None:
    """Run the ETL process."""
    one_hour_ago = datetime.now() - timedelta(hours=1)
    results = extract(one_hour_ago)
    categorized_data = transform_data(results)
    load_data(categorized_data)
```

This main function gives a clear picture of what the ETL process does, and it's much easier to understand before diving into the details.

Note that this new version is not much longer than [v2.py](v2.py), and wouldn't take more time to write. Following a different process while writing the code can greatly improve its quality. It provides several benefits:

- you can catch errors earlier
- you can write tests more easily
- your reviewer will have to spend less time understanding the code, and the likeliness of missing a defect is reduced

## v4: Stronger isolation of concerns with better typing

In real life, the code from v3 may be fine for a while. However, as the system evolves, your database schema may change, and the query will have to be updated. The same goes for the API endpoint, which may change over time.

We can observe that the `transform_data` function makes an assumption on the way the data is structured, to be able to calculate the total amount spent by a user by multiplying the number of purchased items with their unit price, as shown in the snippet below:

```python
for row in results:
    # ... hidden for clarity
    categorized_data[category_id]["data"].append(
        {
            "user_id": row[1],
            "item_name": row[2].upper(),
            "total_spent": row[3] * row[4],
            "timestamp": row[6].isoformat() if row[6] else None,
        }
    )
```

This is a classic example of several code quality issues:

1. **Knowledge Leakage**: The `transform_data` function has implicit knowledge about the database query structure (knowing that `row[1]` is user_id, `row[3]` is quantity, etc.). This creates tight coupling between components that should be independent.

2. **Fragile Dependencies**: If the database schema or query changes (columns are reordered, renamed, or new ones are added), the transformation logic must be updated in perfect sync. This creates a fragile dependency chain where changes in one component necessitate changes in another.

3. **Reduced Testability**: Writing tests for the `transform_data` function becomes problematic because:
   - Tests must construct mock data that exactly matches the SQL query's return format
   - Tests might pass even when the actual query format has changed
   - It's difficult to isolate transformation logic from extraction logic

In real-world applications with complex SQL queries, these issues compound significantly, making the code harder to maintain, test, and evolve. When database schemas inevitably change, these implicit dependencies often lead to subtle bugs that are difficult to detect.

In my own experience working on data platforms, I've encountered SQL queries for reporting that span dozens or even hundreds of lines, with multiple joins across numerous tables, complex aggregations, and intricate filtering logic. In such cases, understanding the exact structure of the result set becomes extremely challenging. When transformation code directly references column positions (like `row[3]`), it becomes nearly impossible to verify correctness without tracing through the entire query. This verification process is not only time-consuming but also error-prone, especially when queries evolve over time to accommodate new business requirements.

These problems could have been avoided from the start by defining a better "contract" between each part of the code - a clear boundary that isolates components and makes their interactions explicit.

Let's explicitly define a new data structure for the data that is passed between the functions:

```python
@dataclass
class Purchase:
    id: int
    timestamp: datetime
    category_id: int
    user_id: int
    item: str
    quantity: int
    price: float
```

This defines a new data structure with a fixed number of properties and typing information, making it clear what data is expected and what is returned.

Now, let's update `extract` to use this type:

```python
def extract(timestamp: datetime) -> List[Purchase]:
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
                for row in cursor.fetchall()
            ]
```

We can now see that the prototype of `extract` is now clear and explicit regarding the type of data it returns. If the SQL query is modified in the future, we only have to update the function and may not have to change the definition of `Purchase`.

Let's update `transform_data` to use this new data structure:

```python
def transform_data(purchases: List[Purchase]) -> Dict[int, Dict[str, Any]]:
    categorized_data = {}
    for purchase in purchases:
        category_id = purchase.category_id

        if category_id not in categorized_data:
            categorized_data[category_id] = {
                "category_id": category_id,
                "data": [],
            }

        categorized_data[category_id]["data"].append(
            {
                "user_id": purchase.user_id,
                "item_name": purchase.item.upper(),
                "total_spent": purchase.quantity * purchase.price,
                "timestamp": purchase.timestamp.isoformat()
                if purchase.timestamp
                else None,
            }
        )
    return categorized_data
```

The calculation of `total_spent` is now significantly clearer as the product of `quantity` and `price`. Purists may argue that this function is still doing too much. The advantage of using a dataclass is that we can consider moving the calculation of `total_spent` to a separate function, which would make the code more maintainable, like this:

```python
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
```

And the final version of `transform_data` would be:

```python
def transform_data(category_id: int, records: List[Purchase]) -> Dict[str, Any]:
    # ...
    for purchase in purchases:
        # ...

        # Transform the data
        categorized_data[category_id]["data"].append(
            {
                "user_id": purchase.user_id,
                "item_name": purchase.item.upper(),
                "total_spent": purchase.total_spent,
                "timestamp": purchase.timestamp.isoformat()
                if purchase.timestamp
                else None,
            }
        )
    return transformed
```

The attentive reader will notice that we have the same dependency and readability problem between the `transform_data` function and the `load_data` one. If we look at the implementation of `send_batch_to_api`, we can see that it assumes the data is a dictionary with a `category_id` key and a `data` key, which is the same as the output of `transform_data`. This creates a dependency that spans multiple levels, from `transform_data` to `load_data` and `send_batch_to_api`.

Let's improve this by creating similar dataclasses for the API data:

```python
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
```

Now, let's update `transform_data` and `load_data` to use this new dataclass:

```python
from dataclasses import asdict

def transform_data(purchases: List[Purchase]) -> Dict[int, APIBatch]:
    categorized_data = {}
    for purchase in purchases:
        category_id = purchase.category_id

        if category_id not in categorized_data:
            categorized_data[category_id] = APIBatch(
                category_id=category_id,
                data=[],
            )

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
    return categorized_data


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
            timeout=30,
        )

        # ...
    except requests.RequestException as e:
        logger.error(f"Category {data.category_id} - API request error: {e}")
        return False


def load_data(categorized_data: Dict[int, APIBatch]) -> None:
    """Send the transformed data to the API.

    Args:
        categorized_data: Dictionary mapping category_id to transformed data
    """
    # ...

    for _, transformed in categorized_data.items():
        if send_batch_to_api(session, transformed):
            success_count += 1
        else:
            failure_count += 1
    # ...

```

These new data structures provide several benefits:

- Clear Contract: These dataclasses create an explicit contract between the transformation and loading steps.
- Type Safety: The type system can now verify that the data being passed has the correct structure.
- Self-Documentation: The code becomes more self-documenting, making it clear what data is expected.
- Improved Testability: It's easier to create test fixtures with well-defined structures.
Better IDE Support: IDEs can provide better autocomplete and error checking.

We can feel happy, we have written "nice code"! But, is it the *right* code?

It's important to pause here and to consider the benefit/cost ratio of this kind of refactoring. This code is better organized and more maintainable, but it's also 50-60% longer than [v3a](v3a.py). For a single-use script, you would probably be wasting time here, and it's important to keep this in mind. However, in a production grade codebase, with a focus on testing, a large part of the coding part will actually be spent on writing tests, so this better organization of the code may actually lead to a net time saving, compared to reaching the same level of test coverage with v2. You may also save time during review. So when working in a team on a production-grade project, this kind of refactoring is probably a good idea.

However, there is no free lunch and if you consider actually running this code, v4 will run at a much higher cost if it processes a non-negligible amount of data. Can you detect the problem?

## Memory consideration

When going through this kind of refactoring exercise, it's very easy to try to write "beautiful code". However, that's never the goal, as software developers we are paid to solve problems and not to create pieces of art.
