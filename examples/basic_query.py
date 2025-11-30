"""Basic query example for Keboola Query Service SDK.

Before running, set these environment variables:
    export KBC_TOKEN="your-storage-api-token"
    export BRANCH_ID="your-branch-id"
    export WORKSPACE_ID="your-workspace-id"

Or create a .env file with these values.
"""

import os

from keboola_query_service import Client

# Configuration
BASE_URL = "https://query.keboola.com"  # Don't use connection.keboola.com
TOKEN = os.environ["KBC_TOKEN"]
BRANCH_ID = os.environ["BRANCH_ID"]
WORKSPACE_ID = os.environ["WORKSPACE_ID"]


def main() -> None:
    # Use context manager for automatic cleanup
    with Client(base_url=BASE_URL, token=TOKEN) as client:
        # Execute a simple query
        results = client.execute_query(
            branch_id=BRANCH_ID,
            workspace_id=WORKSPACE_ID,
            statements=["SELECT 1 as id, 'hello' as message"],
        )

        # Process results
        for i, result in enumerate(results):
            print(f"\n--- Statement {i + 1} ---")
            print(f"Columns: {[col.name for col in result.columns]}")
            print(f"Column types: {[col.type for col in result.columns]}")
            print(f"Row count: {len(result.data)}")
            print("Data:")
            for row in result.data:
                print(f"  {row}")


if __name__ == "__main__":
    main()
