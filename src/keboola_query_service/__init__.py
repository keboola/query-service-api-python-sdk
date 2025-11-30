"""Keboola Query Service Python SDK.

A Python client for the Keboola Query Service API that allows you to execute
SQL queries against Keboola workspaces.

Quick Start
-----------
.. code-block:: python

    from keboola_query_service import Client

    # Initialize client
    # IMPORTANT: Use query.keboola.com (NOT connection.keboola.com)
    client = Client(
        base_url="https://query.keboola.com",
        token="your-storage-api-token"
    )

    # Execute a query
    results = client.execute_query(
        branch_id="1261313",        # Your branch ID
        workspace_id="2950146661",  # Your workspace ID
        statements=["SELECT * FROM my_table LIMIT 10"]
    )

    # Process results
    for result in results:
        print("Columns:", [col.name for col in result.columns])
        print("Data:", result.data)

    client.close()

Using Context Manager (Recommended)
-----------------------------------
.. code-block:: python

    from keboola_query_service import Client

    with Client(base_url="https://query.keboola.com", token="...") as client:
        results = client.execute_query(
            branch_id="1261313",
            workspace_id="2950146661",
            statements=["SELECT 1 as test"]
        )
        print(results[0].data)  # [['1']]

Async Usage
-----------
.. code-block:: python

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

Error Handling
--------------
.. code-block:: python

    from keboola_query_service import (
        Client,
        AuthenticationError,
        ValidationError,
        JobError,
        JobTimeoutError,
    )

    try:
        results = client.execute_query(...)
    except AuthenticationError:
        print("Invalid token")
    except ValidationError as e:
        print(f"Invalid request: {e.message}")
    except JobError as e:
        print(f"Query failed: {e.message}")
    except JobTimeoutError as e:
        print(f"Job {e.job_id} timed out")

Finding Your IDs
----------------
- **base_url**: Use https://query.keboola.com (NOT connection.keboola.com)
- **token**: Settings -> API Tokens in Keboola Connection
- **branch_id**: Found via Storage API or in project URL
- **workspace_id**: Transformations -> Workspace -> Copy ID from URL

Classes
-------
- ``Client`` - Main client for Query Service API
- ``QueryResult`` - Query results with columns and data
- ``JobStatus`` - Job status information
- ``Column`` - Column metadata (name, type)
- ``Statement`` - Statement execution info

Exceptions
----------
- ``QueryServiceError`` - Base exception
- ``AuthenticationError`` - Invalid token (401)
- ``ValidationError`` - Invalid request (400)
- ``NotFoundError`` - Resource not found (404)
- ``JobError`` - Query execution failed
- ``JobTimeoutError`` - Job didn't complete in time
"""

from ._version import __version__
from .client import Client
from .models import (
    ActorType,
    JobState,
    StatementState,
    Column,
    Statement,
    JobStatus,
    QueryResult,
    QueryHistory,
)
from .exceptions import (
    QueryServiceError,
    AuthenticationError,
    ValidationError,
    NotFoundError,
    JobError,
    JobTimeoutError,
)

__all__ = [
    "Client",
    "ActorType",
    "JobState",
    "StatementState",
    "Column",
    "Statement",
    "JobStatus",
    "QueryResult",
    "QueryHistory",
    "QueryServiceError",
    "AuthenticationError",
    "ValidationError",
    "NotFoundError",
    "JobError",
    "JobTimeoutError",
]
