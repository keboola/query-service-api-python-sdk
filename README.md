# Keboola Query Service Python SDK

Python client for [Keboola Query Service API](https://query.keboola.com/api/v1/documentation).

## Installation

```bash
pip install keboola-query-service
```

## Quick Start

```python
from keboola_query_service import Client

# Initialize client
# IMPORTANT: Use query.keboola.com (NOT connection.keboola.com)
# Don't append /api/v1 - the SDK handles routing automatically
client = Client(
    base_url="https://query.keboola.com",  # Query Service URL
    token="your-storage-api-token"          # Your Keboola Storage API token
)

# Execute a query
# - branch_id: Find in Keboola UI URL or via Storage API
# - workspace_id: Your workspace ID from Keboola
results = client.execute_query(
    branch_id="1261313",
    workspace_id="2950146661",
    statements=["SELECT * FROM my_table LIMIT 10"]
)

# Process results - one QueryResult per statement
for result in results:
    print("Columns:", [col.name for col in result.columns])
    print("Data:", result.data)

# Always close the client when done
client.close()
```

### Finding Your IDs

- **branch_id**: Found in the Keboola Connection URL (e.g., `https://connection.keboola.com/admin/projects/123/...` → branch is in the Storage API)
- **workspace_id**: Go to Transformations → Workspace → Copy the workspace ID from URL or details
- **token**: Settings → API Tokens → Create new token with appropriate permissions

## Features

- **Sync and async support** - Both synchronous and async (asyncio) APIs
- **Auto-pagination** - `execute_query` fetches all result pages automatically instead of silently truncating at the API's default page size
- **Automatic retries** - Configurable retry logic for transient failures
- **Job polling** - Built-in exponential backoff for waiting on job completion
- **Streaming** - NDJSON streaming for large result sets
- **Type hints** - Full type annotations for IDE support

## Usage

### Basic Query Execution

```python
from keboola_query_service import Client

with Client(base_url="https://query.keboola.com", token="...") as client:
    # Execute query and wait for results
    results = client.execute_query(
        branch_id="123",
        workspace_id="456",
        statements=[
            "SELECT * FROM orders WHERE date > '2024-01-01'",
            "SELECT COUNT(*) FROM customers"
        ],
        transactional=True  # Execute in a transaction
    )

    # Results is a list - one QueryResult per statement
    orders_result = results[0]
    count_result = results[1]

    print(f"Columns: {[c.name for c in orders_result.columns]}")
    print(f"Rows: {len(orders_result.data)}")
```

### Using Context Manager (Recommended)

```python
from keboola_query_service import Client

# Context manager automatically closes the client
with Client(base_url="https://query.keboola.com", token="...") as client:
    results = client.execute_query(
        branch_id="1261313",
        workspace_id="2950146661",
        statements=["SELECT 1 as test"]
    )
    print(results[0].data)  # [['1']]
```

### Async Usage

```python
import asyncio
from keboola_query_service import Client

async def main():
    async with Client(base_url="https://query.keboola.com", token="...") as client:
        results = await client.execute_query_async(
            branch_id="1261313",
            workspace_id="2950146661",
            statements=["SELECT 1 as test"]
        )
        print(results[0].data)

asyncio.run(main())
```

### Low-Level API

For more control, use the low-level methods:

```python
# Submit job without waiting
job_id = client.submit_job(
    branch_id="123",
    workspace_id="456",
    statements=["SELECT * FROM large_table"]
)

# Check status
status = client.get_job_status(job_id)
print(f"Status: {status.status}")  # created, enqueued, processing, completed, failed

# Wait for completion
final_status = client.wait_for_job(job_id, max_wait_time=300)

# Get results for specific statement
result = client.get_job_results(job_id, final_status.statements[0].id)
```

### Pagination Control

`execute_query` auto-paginates results by default, fetching all rows across
multiple API pages. You can tune the internal page size and cap the total
number of rows returned per statement:

```python
results = client.execute_query(
    branch_id="123",
    workspace_id="456",
    statements=["SELECT * FROM large_table"],
    page_size=1000,   # Internal fetch page size (default 5000)
    max_rows=10000,   # Stop after this many rows per statement
)
```

For the low-level `get_job_results` method, pagination must be handled
manually using `offset` and `page_size` parameters.

### Lazy Iteration (Memory-Efficient)

For large result sets, use `execute_query_iter` to process pages one at a
time without loading everything into memory:

```python
# Yields one QueryResult per page — memory stays bounded
for page in client.execute_query_iter(
    branch_id="123",
    workspace_id="456",
    statements=["SELECT * FROM huge_table"],
):
    for row in page.data:
        process_row(row)
```

`execute_query_iter` defaults to **no row cap** (`max_rows=None`) because
pages are not accumulated in memory.  An async variant
`execute_query_iter_async` is also available.

### Streaming Large Results

```python
# Stream results as NDJSON for large datasets
for row in client.stream_results(job_id, statement_id):
    process_row(row)
```

### Error Handling

```python
from keboola_query_service import (
    Client,
    AuthenticationError,
    ValidationError,
    JobError,
    TimeoutError,
)

try:
    results = client.execute_query(...)
except AuthenticationError:
    print("Invalid token")
except ValidationError as e:
    print(f"Invalid request: {e.message}")
except JobError as e:
    print(f"Query failed: {e.message}")
    for stmt in e.failed_statements:
        print(f"  Statement {stmt['id']}: {stmt['error']}")
except TimeoutError as e:
    print(f"Job {e.job_id} timed out")
```

### Data Apps: Forwarding the App User Token

A data app behind the Keboola apps proxy receives the signed-in user in the
`X-Kbc-App-User-Token` request header. Forward it on every Query Service call
you make for that request. Query Service then applies row-level security for
that user. Without the token, a governed workspace refuses the query.

Keep one shared client and pass the token per call. Never store it on the shared
client, because the client serves all users.

```python
import streamlit as st
from keboola_query_service import (
    Client,
    RowLevelSecurityError,
    user_token_from_headers,
)

@st.cache_resource
def get_client() -> Client:
    return Client(base_url="https://query.keboola.com", token="your-token")

user_token = user_token_from_headers(st.context.headers)

try:
    results = get_client().execute_query(
        branch_id="123",
        workspace_id="456",
        statements=["SELECT * FROM orders"],
        user_token=user_token,
    )
except RowLevelSecurityError:
    st.error("You do not have access to this data.")
```

`client.with_user_token(token)` returns a client for one user. It shares the
connections of the shared client, and closing it does not close them:

```python
from fastapi import Request

@app.get("/orders")
def orders(request: Request):
    user_client = client.with_user_token(user_token_from_headers(request.headers))
    return user_client.execute_query(...)
```

- `user_token_from_headers()` accepts any mapping (Streamlit `st.context.headers`,
  Starlette/FastAPI `request.headers`, a dict). The name is case-insensitive.
  A missing or empty header gives `None`.
- No token (`None` or empty) sends no header, so the call works as before.
- `RowLevelSecurityError` is a `QueryServiceError` (status 403). The SDK raises
  it only for the row-level security refusal of a governed workspace. Other 403
  errors stay `QueryServiceError`.
- A server-side cache of query results must be keyed by the user.

### Query History

```python
history = client.get_query_history(
    branch_id="123",
    workspace_id="456",
    page_size=100
)

for stmt in history.statements:
    print(f"{stmt.query_job_id}: {stmt.query[:50]}... ({stmt.status})")
```

## Configuration

```python
client = Client(
    base_url="https://query.keboola.com",
    token="your-token",
    timeout=120.0,           # Request timeout (seconds)
    connect_timeout=10.0,    # Connection timeout (seconds)
    max_retries=3,           # Max retry attempts
    user_agent="my-app/1.0", # Custom user agent
)
```

## API Reference

### Client Methods

| Method | Description |
|--------|-------------|
| `execute_query()` | Submit query, wait for completion, return all results (auto-paginated) |
| `execute_query_iter()` | Like `execute_query()` but yields pages lazily (memory-efficient, no default row cap) |
| `submit_job()` | Submit query job without waiting |
| `get_job_status()` | Get current job status |
| `get_job_results()` | Get results for a statement |
| `wait_for_job()` | Wait for job to complete |
| `cancel_job()` | Cancel a running job |
| `get_query_history()` | Get query history for workspace |
| `stream_results()` | Stream results as NDJSON |

All methods have async variants with `_async` suffix, and all accept
`user_token=`. `with_user_token(token)` returns a client that sends the token
on every call.

### Models

- `JobStatus` - Job status with statements
- `QueryResult` - Query results with columns and data
- `Statement` - Individual SQL statement info
- `Column` - Column metadata
- `JobState` - Enum: created, enqueued, processing, completed, failed, canceled
- `StatementState` - Enum: waiting, processing, completed, failed, canceled, notExecuted

## License

MIT
