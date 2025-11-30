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
client = Client(
    base_url="https://query.keboola.com",
    token="your-storage-api-token"
)

# Execute a query and wait for results
results = client.execute_query(
    branch_id="123",
    workspace_id="456",
    statements=["SELECT * FROM my_table LIMIT 10"]
)

# Access results
for row in results[0].data:
    print(row)
```

## Features

- **Sync and async support** - Both synchronous and async (asyncio) APIs
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

### Async Usage

```python
import asyncio
from keboola_query_service import Client

async def main():
    async with Client(base_url="...", token="...") as client:
        results = await client.execute_query_async(
            branch_id="123",
            workspace_id="456",
            statements=["SELECT 1"]
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
| `execute_query()` | Submit query, wait for completion, return results |
| `submit_job()` | Submit query job without waiting |
| `get_job_status()` | Get current job status |
| `get_job_results()` | Get results for a statement |
| `wait_for_job()` | Wait for job to complete |
| `cancel_job()` | Cancel a running job |
| `get_query_history()` | Get query history for workspace |
| `stream_results()` | Stream results as NDJSON |

All methods have async variants with `_async` suffix.

### Models

- `JobStatus` - Job status with statements
- `QueryResult` - Query results with columns and data
- `Statement` - Individual SQL statement info
- `Column` - Column metadata
- `JobState` - Enum: created, enqueued, processing, completed, failed, canceled
- `StatementState` - Enum: waiting, processing, completed, failed, canceled, notExecuted

## License

MIT
