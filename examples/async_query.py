"""Async query example for Keboola Query Service SDK.

Before running, set these environment variables:
    export KBC_TOKEN="your-storage-api-token"
    export BRANCH_ID="your-branch-id"
    export WORKSPACE_ID="your-workspace-id"
"""

import asyncio
import os

from keboola_query_service import Client

# Configuration
BASE_URL = "https://query.keboola.com"
TOKEN = os.environ["KBC_TOKEN"]
BRANCH_ID = os.environ["BRANCH_ID"]
WORKSPACE_ID = os.environ["WORKSPACE_ID"]


async def run_query(client: Client, query: str, name: str) -> None:
    """Run a single query and print results."""
    print(f"\n[{name}] Executing: {query}")

    results = await client.execute_query_async(
        branch_id=BRANCH_ID,
        workspace_id=WORKSPACE_ID,
        statements=[query],
    )

    print(f"[{name}] Columns: {[col.name for col in results[0].columns]}")
    print(f"[{name}] Data: {results[0].data}")


async def main() -> None:
    async with Client(base_url=BASE_URL, token=TOKEN) as client:
        # Run multiple queries concurrently
        await asyncio.gather(
            run_query(client, "SELECT 1 as num", "Query A"),
            run_query(client, "SELECT 'hello' as msg", "Query B"),
            run_query(client, "SELECT CURRENT_TIMESTAMP() as ts", "Query C"),
        )


if __name__ == "__main__":
    asyncio.run(main())
