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
    CredentialError,
    ValidationError,
    JobError,
    TimeoutError,
)

try:
    results = client.execute_query(...)
except CredentialError as e:
    print(f"The credential cannot be used, nothing was sent: {e.message}")
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

### Credentials

`token="..."` is short for `auth=StorageToken("...")`. Use `auth=` for the other
credentials:

| Credential | Headers |
|------------|---------|
| `StorageToken(token)` | `X-StorageAPI-Token` |
| `BearerToken(token, project_id)` | `Authorization: Bearer <token>` and `X-KBC-ProjectId` (programmatic tokens `kbc_pat_`/`kbc_at_`) |
| `AppUserToken(token, storage_token=StorageToken(...))` | `X-Kbc-App-User-Token` and `X-StorageAPI-Token` (works with any workspace) |

A request always sends the headers of exactly one credential. `AppUserToken` is
the app user token together with the app's Storage token, so it sends both
headers; the Storage token argument is required.
`credential_headers()` gives the headers of a credential.

There are two levels, and both are read before every request, retries included,
in the sync and the async methods:

- **The token of a credential** is a value or a callable with no arguments that
  returns the current value. The SDK checks the value each time (non-empty,
  printable ASCII, no spaces) and raises `CredentialError` before it sends
  anything. `CredentialError` is not a `ValidationError` (a 400 from the server),
  so a handler for bad queries never reports a token problem.
- **The credential of the client** (`auth=`) is a credential or a **provider**:
  a callable with no arguments that returns a credential.

`execute_query_iter()` and `stream_results()` read them when you start reading,
not when you call them.

**A bad credential fails when the client is created.** `Client(url, "")` or a
`token` that cannot be sent raises `CredentialError` in the constructor, not a 401
on the first request, so code that builds a client at import time from an unset
environment variable now fails there. A callable that later returns an empty or
unsendable value raises `CredentialError` when it is read, before the request.
Credentials are immutable: `copy.copy()` and `copy.deepcopy()` return the same
object, and pickling one raises `TypeError` on purpose, as a token must never be
serialised.

#### 1. A static token: scripts, jobs, single-tenant servers

```python
import os

from keboola_query_service import BearerToken, Client, StorageToken

client = Client(
    base_url="https://query.keboola.com",
    auth=StorageToken(os.environ["KBC_TOKEN"]),
)

# A programmatic token (kbc_pat_/kbc_at_) needs the project ID:
pat_client = Client(
    base_url="https://query.keboola.com",
    auth=BearerToken(os.environ["KBC_PAT"], project_id=123),
)
```

#### 2. A token that changes: a rotated token file

```python
from pathlib import Path

from keboola_query_service import Client, StorageToken

token_file = Path("/var/run/secrets/kbc/token")

client = Client(
    base_url="https://query.keboola.com",
    auth=StorageToken(lambda: token_file.read_text().strip()),
)
```

Each request reads the file again, so a new token is used from the next request.

#### 3. A server with a credential for each incoming request

A middleware puts the credential of the request into a `ContextVar`. One shared
client reads it through a provider. Each request and each retry calls the
provider, so one shared client is safe for many users at the same time.

This FastAPI example sends the app user token together with the server's Storage
token when the request has the `X-Kbc-App-User-Token` header, and only the
server's Storage token when it does not:

```python
import os
from collections.abc import Awaitable, Callable
from contextvars import ContextVar

from fastapi import FastAPI, Request, Response

from keboola_query_service import AppUserToken, Client, Credential, StorageToken

server_auth = StorageToken(os.environ["KBC_TOKEN"])
current_auth: ContextVar[Credential] = ContextVar("current_auth", default=server_auth)

client = Client(
    base_url=os.environ["QUERY_SERVICE_URL"],
    auth=lambda: current_auth.get(),
)

app = FastAPI()


@app.middleware("http")
async def set_query_service_auth(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    app_user_token = request.headers.get("X-Kbc-App-User-Token")
    token = current_auth.set(
        AppUserToken(app_user_token, storage_token=server_auth)
        if app_user_token
        else server_auth
    )
    try:
        return await call_next(request)
    finally:
        current_auth.reset(token)


@app.get("/orders")
def orders() -> list[list[object]]:
    results = client.execute_query(
        branch_id=os.environ["BRANCH_ID"],
        workspace_id=os.environ["WORKSPACE_ID"],
        statements=["SELECT * FROM orders"],
    )
    return results[0].data
```

Code outside a request (start-up code, background jobs) gets the default, the
server's Storage token.

#### 4. A per-call override

Every method that calls Query Service takes a keyword-only `auth=` (a
credential or a provider). It replaces the client's `auth` for that one call.
The client does not store it:

```python
results = client.execute_query(
    branch_id="123",
    workspace_id="456",
    statements=["SELECT * FROM orders"],
    auth=AppUserToken(app_user_token, storage_token=server_auth),
)
```

The SDK never logs a token and never puts one into `repr()` or an error.

### Row-Level Security Refusal

When row-level security refuses a query, the SDK raises `RowLevelSecurityError`,
a subclass of `QueryServiceError` (status 403). A governed workspace refuses
every query whose caller matches no policy. A call with only the Storage API
token gets this error too. Other errors do not change.

Query Service does not refuse every caller this way (an older Query Service
refuses an app user too; the behavior below comes with its next release). For a table with no policy,
or a policy that selects the caller by no rule and has no default, an
**administrator** (a Storage API token) gets `RowLevelSecurityError`, but an
**app user** (an app user token) gets a successful result with no rows, so for an
app user an empty result can also mean "no access". `RowLevelSecurityError` is
raised only where Query Service refuses: an administrator, a statement that is not
allowed in a governed workspace (such as `SHOW` or `INFORMATION_SCHEMA`), and a
table that a column policy withholds from the caller.

```python
from keboola_query_service import RowLevelSecurityError

try:
    results = client.execute_query(
        branch_id="123",
        workspace_id="456",
        statements=["SELECT * FROM orders"],
    )
except RowLevelSecurityError:
    print("No access to this data")
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
`auth=`.

### Models

- `JobStatus` - Job status with statements
- `QueryResult` - Query results with columns and data
- `Statement` - Individual SQL statement info
- `Column` - Column metadata
- `JobState` - Enum: created, enqueued, processing, completed, failed, canceled
- `StatementState` - Enum: waiting, processing, completed, failed, canceled, notExecuted

## License

MIT
