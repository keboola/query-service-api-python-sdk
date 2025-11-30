"""Error handling example for Keboola Query Service SDK.

Demonstrates how to handle various error types.
"""

import os

from keboola_query_service import (
    Client,
    AuthenticationError,
    ValidationError,
    JobError,
    JobTimeoutError,
    NotFoundError,
    QueryServiceError,
)

BASE_URL = "https://query.keboola.com"
TOKEN = os.environ["KBC_TOKEN"]
BRANCH_ID = os.environ["BRANCH_ID"]
WORKSPACE_ID = os.environ["WORKSPACE_ID"]


def main() -> None:
    with Client(base_url=BASE_URL, token=TOKEN) as client:
        try:
            # This query will fail due to invalid SQL
            results = client.execute_query(
                branch_id=BRANCH_ID,
                workspace_id=WORKSPACE_ID,
                statements=["SELECT * FROM nonexistent_table_xyz"],
            )
            print("Results:", results)

        except AuthenticationError as e:
            # Invalid or expired token
            print(f"Authentication failed: {e.message}")
            print(f"  Status code: {e.status_code}")

        except ValidationError as e:
            # Invalid request parameters
            print(f"Validation error: {e.message}")
            print(f"  Context: {e.context}")

        except NotFoundError as e:
            # Resource not found (job, workspace, etc.)
            print(f"Not found: {e.message}")

        except JobError as e:
            # Query execution failed
            print(f"Job failed: {e.message}")
            print(f"  Job ID: {e.job_id}")
            if e.failed_statements:
                for stmt in e.failed_statements:
                    print(f"  Statement {stmt['id']}: {stmt['error']}")

        except JobTimeoutError as e:
            # Job didn't complete in time
            print(f"Timeout: {e.message}")
            print(f"  Job ID: {e.job_id}")
            # You can cancel the job here if needed
            # client.cancel_job(e.job_id)

        except QueryServiceError as e:
            # Generic error (5xx, network issues, etc.)
            print(f"Error: {e.message}")
            print(f"  Status code: {e.status_code}")
            print(f"  Exception ID: {e.exception_id}")


if __name__ == "__main__":
    main()
