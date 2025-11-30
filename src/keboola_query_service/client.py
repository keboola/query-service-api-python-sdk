"""Keboola Query Service Client."""

import asyncio
import logging
import random
import time
from typing import Any, AsyncIterator, Iterator, cast

import httpx

from ._version import __version__
from .exceptions import (
    AuthenticationError,
    JobError,
    NotFoundError,
    QueryServiceError,
    JobTimeoutError,
    ValidationError,
)
from .models import (
    ActorType,
    JobState,
    JobStatus,
    QueryHistory,
    QueryResult,
)

logger = logging.getLogger(__name__)


class Client:
    """Client for Keboola Query Service API.

    Important: Use either sync methods OR async methods, not both on the same client instance.
    For sync operations, use context manager `with Client(...) as client:` or call `close()`.
    For async operations, use `async with Client(...) as client:` or `await close_async()`.

    Example (sync):
        >>> with Client(
        ...     base_url="https://query.keboola.com",
        ...     token="your-storage-api-token"
        ... ) as client:
        ...     result = client.execute_query(
        ...         branch_id="123",
        ...         workspace_id="456",
        ...         statements=["SELECT * FROM my_table"]
        ...     )
        ...     print(result.data)

    Example (async):
        >>> async with Client(
        ...     base_url="https://query.keboola.com",
        ...     token="your-storage-api-token"
        ... ) as client:
        ...     result = await client.execute_query_async(
        ...         branch_id="123",
        ...         workspace_id="456",
        ...         statements=["SELECT * FROM my_table"]
        ...     )
        ...     print(result.data)
    """

    DEFAULT_TIMEOUT = 120.0
    DEFAULT_CONNECT_TIMEOUT = 10.0
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_POLL_INTERVAL_START = 0.1  # 100ms
    DEFAULT_POLL_INTERVAL_MAX = 2.0  # 2s
    DEFAULT_MAX_WAIT_TIME = 300.0  # 5 minutes

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        user_agent: str | None = None,
    ):
        """Initialize the client.

        Args:
            base_url: Base URL of the Query Service (e.g., "https://query.keboola.com")
            token: Keboola Storage API token
            timeout: Request timeout in seconds
            connect_timeout: Connection timeout in seconds
            max_retries: Maximum number of retries for failed requests
            user_agent: Custom user agent string
        """
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.max_retries = max_retries
        self._user_agent = user_agent

        self._client = httpx.Client(
            base_url=self.base_url,
            headers=self._build_headers(user_agent),
            timeout=httpx.Timeout(timeout, connect=connect_timeout),
        )
        self._async_client: httpx.AsyncClient | None = None

    def _build_headers(self, user_agent: str | None) -> dict[str, str]:
        """Build request headers."""
        return {
            "X-StorageAPI-Token": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": user_agent or f"keboola-query-service-python-sdk/{__version__}",
        }

    def _get_async_client(self) -> httpx.AsyncClient:
        """Get or create async client."""
        if self._async_client is None:
            self._async_client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=self._build_headers(self._user_agent),
                timeout=self._client.timeout,
            )
        return self._async_client

    def close(self) -> None:
        """Close the client and release resources.

        Closes the sync client and clears the async client reference.
        If you used async methods, call close_async() instead for proper cleanup.
        """
        self._client.close()
        # Set async client to None - it will be garbage collected
        # For proper async cleanup, use close_async() instead
        self._async_client = None

    async def close_async(self) -> None:
        """Close both sync and async clients (async version).

        This is the preferred cleanup method if you used any async methods.
        Properly closes both clients to avoid resource leaks.
        """
        # Close sync client
        self._client.close()

        # Close async client if it was created
        if self._async_client:
            await self._async_client.aclose()
            self._async_client = None

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close_async()

    def _handle_error(self, response: httpx.Response) -> None:
        """Handle error responses."""
        try:
            error_data = response.json()
            message = error_data.get("exception", response.text)
            exception_id = error_data.get("exceptionId")
            context = error_data.get("context")
        except Exception:
            message = response.text
            exception_id = None
            context = None

        kwargs = {
            "message": message,
            "status_code": response.status_code,
            "exception_id": exception_id,
            "context": context,
        }

        if response.status_code == 401:
            raise AuthenticationError(**kwargs)
        elif response.status_code == 400:
            raise ValidationError(**kwargs)
        elif response.status_code == 404:
            raise NotFoundError(**kwargs)
        else:
            raise QueryServiceError(**kwargs)

    def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Make an HTTP request with retry logic."""
        last_exception: Exception | None = None
        last_response: httpx.Response | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.request(method, path, **kwargs)

                # Check if we should retry based on status code
                should_retry = (
                    response.status_code >= 500 or response.status_code == 429
                )

                if response.status_code >= 400:
                    response.read()

                    # Retry on 5xx and 429, otherwise raise error
                    if should_retry and attempt < self.max_retries:
                        last_response = response
                        wait_time = min(2**attempt * 0.1, 10)
                        # Add jitter (0-100ms)
                        jitter = random.uniform(0, 0.1)
                        total_wait = wait_time + jitter
                        logger.warning(
                            f"Request failed with status {response.status_code} "
                            f"(attempt {attempt + 1}/{self.max_retries + 1}), "
                            f"retrying in {total_wait:.2f}s"
                        )
                        time.sleep(total_wait)
                        continue

                    self._handle_error(response)

                return cast(dict[str, Any], response.json())

            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_exception = e
                if attempt < self.max_retries:
                    wait_time = min(2**attempt * 0.1, 10)
                    # Add jitter (0-100ms)
                    jitter = random.uniform(0, 0.1)
                    total_wait = wait_time + jitter
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{self.max_retries + 1}), "
                        f"retrying in {total_wait:.2f}s: {e}"
                    )
                    time.sleep(total_wait)
                    continue

        # If we got here, all retries failed
        if last_response is not None:
            # Last retry was a 5xx or 429
            self._handle_error(last_response)

        raise QueryServiceError(
            f"Request failed after {self.max_retries + 1} attempts: {last_exception}"
        )

    async def _request_async(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Make an async HTTP request with retry logic."""
        client = self._get_async_client()
        last_exception: Exception | None = None
        last_response: httpx.Response | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = await client.request(method, path, **kwargs)

                # Check if we should retry based on status code
                should_retry = (
                    response.status_code >= 500 or response.status_code == 429
                )

                if response.status_code >= 400:
                    await response.aread()

                    # Retry on 5xx and 429, otherwise raise error
                    if should_retry and attempt < self.max_retries:
                        last_response = response
                        wait_time = min(2**attempt * 0.1, 10)
                        # Add jitter (0-100ms)
                        jitter = random.uniform(0, 0.1)
                        total_wait = wait_time + jitter
                        logger.warning(
                            f"Request failed with status {response.status_code} "
                            f"(attempt {attempt + 1}/{self.max_retries + 1}), "
                            f"retrying in {total_wait:.2f}s"
                        )
                        await asyncio.sleep(total_wait)
                        continue

                    self._handle_error(response)

                return cast(dict[str, Any], response.json())

            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_exception = e
                if attempt < self.max_retries:
                    wait_time = min(2**attempt * 0.1, 10)
                    # Add jitter (0-100ms)
                    jitter = random.uniform(0, 0.1)
                    total_wait = wait_time + jitter
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{self.max_retries + 1}), "
                        f"retrying in {total_wait:.2f}s: {e}"
                    )
                    await asyncio.sleep(total_wait)
                    continue

        # If we got here, all retries failed
        if last_response is not None:
            # Last retry was a 5xx or 429
            self._handle_error(last_response)

        raise QueryServiceError(
            f"Request failed after {self.max_retries + 1} attempts: {last_exception}"
        )

    # =========================================================================
    # Low-level API methods
    # =========================================================================

    def submit_job(
        self,
        branch_id: str,
        workspace_id: str,
        statements: list[str],
        *,
        transactional: bool = True,
        actor_type: ActorType = ActorType.USER,
    ) -> str:
        """Submit a query job without waiting for completion.

        Args:
            branch_id: Branch ID
            workspace_id: Workspace ID
            statements: List of SQL statements to execute
            transactional: Whether to execute statements in a transaction
            actor_type: Actor type (user or system)

        Returns:
            Query job ID
        """
        data = self._request(
            "POST",
            f"/api/v1/branches/{branch_id}/workspaces/{workspace_id}/queries",
            json={
                "statements": statements,
                "transactional": transactional,
                "actorType": actor_type.value,
            },
        )
        return str(data["queryJobId"])

    async def submit_job_async(
        self,
        branch_id: str,
        workspace_id: str,
        statements: list[str],
        *,
        transactional: bool = True,
        actor_type: ActorType = ActorType.USER,
    ) -> str:
        """Submit a query job without waiting (async version)."""
        data = await self._request_async(
            "POST",
            f"/api/v1/branches/{branch_id}/workspaces/{workspace_id}/queries",
            json={
                "statements": statements,
                "transactional": transactional,
                "actorType": actor_type.value,
            },
        )
        return str(data["queryJobId"])

    def get_job_status(self, query_job_id: str) -> JobStatus:
        """Get the status of a query job.

        Args:
            query_job_id: Query job ID

        Returns:
            JobStatus with current state and statements
        """
        data = self._request("GET", f"/api/v1/queries/{query_job_id}")
        return JobStatus.from_dict(data)

    async def get_job_status_async(self, query_job_id: str) -> JobStatus:
        """Get the status of a query job (async version)."""
        data = await self._request_async("GET", f"/api/v1/queries/{query_job_id}")
        return JobStatus.from_dict(data)

    def get_job_results(
        self,
        query_job_id: str,
        statement_id: str,
        *,
        offset: int = 0,
        page_size: int = 500,
    ) -> QueryResult:
        """Get results for a specific statement.

        Args:
            query_job_id: Query job ID
            statement_id: Statement ID
            offset: Offset for pagination
            page_size: Page size for pagination

        Returns:
            QueryResult with columns and data
        """
        data = self._request(
            "GET",
            f"/api/v1/queries/{query_job_id}/{statement_id}/results",
            params={"offset": offset, "pageSize": page_size},
        )
        return QueryResult.from_dict(data)

    async def get_job_results_async(
        self,
        query_job_id: str,
        statement_id: str,
        *,
        offset: int = 0,
        page_size: int = 500,
    ) -> QueryResult:
        """Get results for a specific statement (async version)."""
        data = await self._request_async(
            "GET",
            f"/api/v1/queries/{query_job_id}/{statement_id}/results",
            params={"offset": offset, "pageSize": page_size},
        )
        return QueryResult.from_dict(data)

    def cancel_job(
        self,
        query_job_id: str,
        reason: str | None = None,
    ) -> str:
        """Cancel a running query job.

        Args:
            query_job_id: Query job ID
            reason: Optional cancellation reason

        Returns:
            Query job ID
        """
        data = self._request(
            "POST",
            f"/api/v1/queries/{query_job_id}/cancel",
            json={"reason": reason or "Canceled by user"},
        )
        return str(data["queryJobId"])

    async def cancel_job_async(
        self,
        query_job_id: str,
        reason: str | None = None,
    ) -> str:
        """Cancel a running query job (async version)."""
        data = await self._request_async(
            "POST",
            f"/api/v1/queries/{query_job_id}/cancel",
            json={"reason": reason or "Canceled by user"},
        )
        return str(data["queryJobId"])

    def get_query_history(
        self,
        branch_id: str,
        workspace_id: str,
        *,
        after_id: str | None = None,
        page_size: int = 500,
    ) -> QueryHistory:
        """Get query history for a workspace.

        Args:
            branch_id: Branch ID
            workspace_id: Workspace ID
            after_id: Get results after this statement ID
            page_size: Number of results per page

        Returns:
            QueryHistory with list of statements
        """
        params: dict[str, Any] = {"pageSize": page_size}
        if after_id:
            params["afterId"] = after_id

        data = self._request(
            "GET",
            f"/api/v1/branches/{branch_id}/workspaces/{workspace_id}/queries",
            params=params,
        )
        return QueryHistory.from_dict(data)

    async def get_query_history_async(
        self,
        branch_id: str,
        workspace_id: str,
        *,
        after_id: str | None = None,
        page_size: int = 500,
    ) -> QueryHistory:
        """Get query history for a workspace (async version)."""
        params: dict[str, Any] = {"pageSize": page_size}
        if after_id:
            params["afterId"] = after_id

        data = await self._request_async(
            "GET",
            f"/api/v1/branches/{branch_id}/workspaces/{workspace_id}/queries",
            params=params,
        )
        return QueryHistory.from_dict(data)

    # =========================================================================
    # High-level convenience methods
    # =========================================================================

    def wait_for_job(
        self,
        query_job_id: str,
        *,
        max_wait_time: float = DEFAULT_MAX_WAIT_TIME,
        poll_interval_start: float = DEFAULT_POLL_INTERVAL_START,
        poll_interval_max: float = DEFAULT_POLL_INTERVAL_MAX,
    ) -> JobStatus:
        """Wait for a job to complete.

        Args:
            query_job_id: Query job ID
            max_wait_time: Maximum time to wait in seconds
            poll_interval_start: Initial polling interval
            poll_interval_max: Maximum polling interval

        Returns:
            Final JobStatus

        Raises:
            JobTimeoutError: If job doesn't complete within max_wait_time
            JobError: If job fails
        """
        start_time = time.time()
        poll_interval = poll_interval_start

        while True:
            status = self.get_job_status(query_job_id)

            if status.status.is_terminal():
                if status.status == JobState.FAILED:
                    raise JobError(
                        message=status.get_first_error() or "Job failed",
                        job_id=query_job_id,
                        failed_statements=[
                            {"id": s.id, "error": s.error}
                            for s in status.get_failed_statements()
                        ],
                    )
                return status

            elapsed = time.time() - start_time
            if elapsed >= max_wait_time:
                raise JobTimeoutError(
                    message=f"Job did not complete within {max_wait_time}s",
                    job_id=query_job_id,
                )

            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, poll_interval_max)

    async def wait_for_job_async(
        self,
        query_job_id: str,
        *,
        max_wait_time: float = DEFAULT_MAX_WAIT_TIME,
        poll_interval_start: float = DEFAULT_POLL_INTERVAL_START,
        poll_interval_max: float = DEFAULT_POLL_INTERVAL_MAX,
    ) -> JobStatus:
        """Wait for a job to complete (async version)."""
        start_time = time.time()
        poll_interval = poll_interval_start

        while True:
            status = await self.get_job_status_async(query_job_id)

            if status.status.is_terminal():
                if status.status == JobState.FAILED:
                    raise JobError(
                        message=status.get_first_error() or "Job failed",
                        job_id=query_job_id,
                        failed_statements=[
                            {"id": s.id, "error": s.error}
                            for s in status.get_failed_statements()
                        ],
                    )
                return status

            elapsed = time.time() - start_time
            if elapsed >= max_wait_time:
                raise JobTimeoutError(
                    message=f"Job did not complete within {max_wait_time}s",
                    job_id=query_job_id,
                )

            await asyncio.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, poll_interval_max)

    def execute_query(
        self,
        branch_id: str,
        workspace_id: str,
        statements: list[str],
        *,
        transactional: bool = True,
        actor_type: ActorType = ActorType.USER,
        max_wait_time: float = DEFAULT_MAX_WAIT_TIME,
    ) -> list[QueryResult]:
        """Execute query and wait for results.

        This is a convenience method that submits a job, waits for completion,
        and fetches results for all statements.

        Args:
            branch_id: Branch ID
            workspace_id: Workspace ID
            statements: List of SQL statements to execute
            transactional: Whether to execute in a transaction
            actor_type: Actor type
            max_wait_time: Maximum time to wait for completion

        Returns:
            List of QueryResult, one per statement

        Raises:
            JobError: If job fails
            JobTimeoutError: If job doesn't complete in time
        """
        # Submit job
        job_id = self.submit_job(
            branch_id=branch_id,
            workspace_id=workspace_id,
            statements=statements,
            transactional=transactional,
            actor_type=actor_type,
        )

        # Wait for completion
        status = self.wait_for_job(job_id, max_wait_time=max_wait_time)

        # Fetch results for each statement
        results = []
        for statement in status.statements:
            result = self.get_job_results(job_id, statement.id)
            results.append(result)

        return results

    async def execute_query_async(
        self,
        branch_id: str,
        workspace_id: str,
        statements: list[str],
        *,
        transactional: bool = True,
        actor_type: ActorType = ActorType.USER,
        max_wait_time: float = DEFAULT_MAX_WAIT_TIME,
    ) -> list[QueryResult]:
        """Execute query and wait for results (async version)."""
        # Submit job
        job_id = await self.submit_job_async(
            branch_id=branch_id,
            workspace_id=workspace_id,
            statements=statements,
            transactional=transactional,
            actor_type=actor_type,
        )

        # Wait for completion
        status = await self.wait_for_job_async(job_id, max_wait_time=max_wait_time)

        # Fetch results for each statement
        results = []
        for statement in status.statements:
            result = await self.get_job_results_async(job_id, statement.id)
            results.append(result)

        return results

    def stream_results(
        self,
        query_job_id: str,
        statement_id: str,
    ) -> Iterator[dict[str, Any]]:
        """Stream results as NDJSON.

        Args:
            query_job_id: Query job ID
            statement_id: Statement ID

        Yields:
            Parsed JSON objects from the NDJSON stream
        """
        with self._client.stream(
            "GET",
            f"/api/v1/queries/{query_job_id}/{statement_id}/results/stream",
        ) as response:
            if response.status_code >= 400:
                # Read full response for error handling
                response.read()
                self._handle_error(response)

            import json

            for line in response.iter_lines():
                if line:
                    yield json.loads(line)

    async def stream_results_async(
        self,
        query_job_id: str,
        statement_id: str,
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream results as NDJSON (async version)."""
        client = self._get_async_client()

        async with client.stream(
            "GET",
            f"/api/v1/queries/{query_job_id}/{statement_id}/results/stream",
        ) as response:
            if response.status_code >= 400:
                await response.aread()
                self._handle_error(response)

            import json

            async for line in response.aiter_lines():
                if line:
                    yield json.loads(line)
