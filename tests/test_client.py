"""Tests for Keboola Query Service Client."""

import json

import pytest
from pytest_httpx import HTTPXMock

from keboola_query_service import (
    AuthenticationError,
    Client,
    JobError,
    JobState,
    StatementState,
    ValidationError,
)

_DEFAULT_PAGE_SIZE = Client.DEFAULT_EXECUTE_QUERY_PAGE_SIZE


@pytest.fixture
def client():
    """Create a test client."""
    return Client(
        base_url="https://query.test.keboola.com",
        token="test-token",
    )


class TestSubmitJob:
    def test_submit_job_success(self, client: Client, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="POST",
            url="https://query.test.keboola.com/api/v1/branches/123/workspaces/456/queries",
            json={"queryJobId": "job-abc123"},
            status_code=201,
        )

        job_id = client.submit_job(
            branch_id="123",
            workspace_id="456",
            statements=["SELECT * FROM test"],
        )

        assert job_id == "job-abc123"

    def test_submit_job_auth_error(self, client: Client, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="POST",
            url="https://query.test.keboola.com/api/v1/branches/123/workspaces/456/queries",
            json={"exception": "Invalid token", "exceptionId": "err-123"},
            status_code=401,
        )

        with pytest.raises(AuthenticationError) as exc_info:
            client.submit_job(
                branch_id="123",
                workspace_id="456",
                statements=["SELECT * FROM test"],
            )

        assert exc_info.value.status_code == 401
        assert exc_info.value.exception_id == "err-123"

    def test_submit_job_validation_error(self, client: Client, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="POST",
            url="https://query.test.keboola.com/api/v1/branches/123/workspaces/456/queries",
            json={"exception": "Statements must not be empty"},
            status_code=400,
        )

        with pytest.raises(ValidationError):
            client.submit_job(
                branch_id="123",
                workspace_id="456",
                statements=[],
            )


class TestGetJobStatus:
    def test_get_job_status_success(self, client: Client, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="GET",
            url="https://query.test.keboola.com/api/v1/queries/job-abc123",
            json={
                "queryJobId": "job-abc123",
                "status": "completed",
                "actorType": "user",
                "createdAt": "2024-01-01T00:00:00Z",
                "changedAt": "2024-01-01T00:01:00Z",
                "statements": [
                    {
                        "id": "stmt-1",
                        "query": "SELECT * FROM test",
                        "status": "completed",
                        "rowsAffected": 100,
                    }
                ],
            },
        )

        status = client.get_job_status("job-abc123")

        assert status.query_job_id == "job-abc123"
        assert status.status == JobState.COMPLETED
        assert len(status.statements) == 1
        assert status.statements[0].status == StatementState.COMPLETED


class TestGetJobResults:
    def test_get_job_results_success(self, client: Client, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="GET",
            url="https://query.test.keboola.com/api/v1/queries/job-abc123/stmt-1/results?offset=0&pageSize=500",
            json={
                "status": "completed",
                "columns": [
                    {"name": "id", "type": "NUMBER", "nullable": False, "length": 38},
                    {
                        "name": "name",
                        "type": "VARCHAR",
                        "nullable": True,
                        "length": 256,
                    },
                ],
                "data": [
                    ["1", "Alice"],
                    ["2", "Bob"],
                ],
                "numberOfRows": 2,
                "rowsAffected": 0,
            },
        )

        result = client.get_job_results("job-abc123", "stmt-1")

        assert result.status == StatementState.COMPLETED
        assert len(result.columns) == 2
        assert result.columns[0].name == "id"
        assert len(result.data) == 2
        assert result.data[0] == ["1", "Alice"]


class TestCancelJob:
    def test_cancel_job_success(self, client: Client, httpx_mock: HTTPXMock):
        httpx_mock.add_response(
            method="POST",
            url="https://query.test.keboola.com/api/v1/queries/job-abc123/cancel",
            json={"queryJobId": "job-abc123"},
        )

        job_id = client.cancel_job("job-abc123", reason="Test cancellation")

        assert job_id == "job-abc123"


class TestExecuteQuery:
    def test_execute_query_success(self, client: Client, httpx_mock: HTTPXMock):
        # Mock submit
        httpx_mock.add_response(
            method="POST",
            url="https://query.test.keboola.com/api/v1/branches/123/workspaces/456/queries",
            json={"queryJobId": "job-abc123"},
            status_code=201,
        )

        # Mock status (completed immediately)
        httpx_mock.add_response(
            method="GET",
            url="https://query.test.keboola.com/api/v1/queries/job-abc123",
            json={
                "queryJobId": "job-abc123",
                "status": "completed",
                "actorType": "user",
                "createdAt": "2024-01-01T00:00:00Z",
                "changedAt": "2024-01-01T00:01:00Z",
                "statements": [
                    {
                        "id": "stmt-1",
                        "query": "SELECT 1",
                        "status": "completed",
                    }
                ],
            },
        )

        # Mock results
        httpx_mock.add_response(
            method="GET",
            url=f"https://query.test.keboola.com/api/v1/queries/job-abc123/stmt-1/results?offset=0&pageSize={_DEFAULT_PAGE_SIZE}",
            json={
                "status": "completed",
                "columns": [
                    {"name": "1", "type": "NUMBER", "nullable": False, "length": 1}
                ],
                "data": [["1"]],
                "numberOfRows": 1,
            },
        )

        results = client.execute_query(
            branch_id="123",
            workspace_id="456",
            statements=["SELECT 1"],
        )

        assert len(results) == 1
        assert results[0].data == [["1"]]

    def test_execute_query_job_fails(self, client: Client, httpx_mock: HTTPXMock):
        # Mock submit
        httpx_mock.add_response(
            method="POST",
            url="https://query.test.keboola.com/api/v1/branches/123/workspaces/456/queries",
            json={"queryJobId": "job-abc123"},
            status_code=201,
        )

        # Mock status (failed)
        httpx_mock.add_response(
            method="GET",
            url="https://query.test.keboola.com/api/v1/queries/job-abc123",
            json={
                "queryJobId": "job-abc123",
                "status": "failed",
                "actorType": "user",
                "createdAt": "2024-01-01T00:00:00Z",
                "changedAt": "2024-01-01T00:01:00Z",
                "statements": [
                    {
                        "id": "stmt-1",
                        "query": "SELECT * FROM nonexistent",
                        "status": "failed",
                        "error": "Table 'nonexistent' does not exist",
                    }
                ],
            },
        )

        with pytest.raises(JobError) as exc_info:
            client.execute_query(
                branch_id="123",
                workspace_id="456",
                statements=["SELECT * FROM nonexistent"],
            )

        assert exc_info.value.job_id == "job-abc123"
        assert "does not exist" in str(exc_info.value)


_SUBMIT_URL = (
    "https://query.test.keboola.com/api/v1/branches/123/workspaces/456/queries"
)
_STATUS_URL = "https://query.test.keboola.com/api/v1/queries/job-1"
_RESULTS_URL = f"https://query.test.keboola.com/api/v1/queries/job-1/stmt-1/results?offset=0&pageSize={_DEFAULT_PAGE_SIZE}"

_STATUS_COMPLETED = {
    "queryJobId": "job-1",
    "status": "completed",
    "actorType": "user",
    "createdAt": "2024-01-01T00:00:00Z",
    "changedAt": "2024-01-01T00:01:00Z",
    "statements": [{"id": "stmt-1", "query": "SELECT 1", "status": "completed"}],
}

_RESULTS_COMPLETED = {
    "status": "completed",
    "columns": [{"name": "1", "type": "NUMBER", "nullable": False, "length": 1}],
    "data": [["1"]],
    "numberOfRows": 1,
}


class TestSessionId:
    def test_submit_job_sends_session_id(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=_SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        client.submit_job("123", "456", ["SELECT 1"], session_id="sess-123")
        body = json.loads(httpx_mock.get_requests()[0].content)
        assert body["sessionId"] == "sess-123"

    def test_submit_job_omits_session_id_by_default(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=_SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        client.submit_job("123", "456", ["SELECT 1"])
        body = json.loads(httpx_mock.get_requests()[0].content)
        assert "sessionId" not in body

    async def test_submit_job_async_sends_session_id(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=_SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        await client.submit_job_async(
            "123", "456", ["SELECT 1"], session_id="sess-async"
        )
        body = json.loads(httpx_mock.get_requests()[0].content)
        assert body["sessionId"] == "sess-async"

    def test_execute_query_forwards_session_id(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=_SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        httpx_mock.add_response(method="GET", url=_STATUS_URL, json=_STATUS_COMPLETED)
        httpx_mock.add_response(method="GET", url=_RESULTS_URL, json=_RESULTS_COMPLETED)
        client.execute_query("123", "456", ["SELECT 1"], session_id="sess-exec")
        body = json.loads(httpx_mock.get_requests()[0].content)
        assert body["sessionId"] == "sess-exec"

    async def test_execute_query_async_forwards_session_id(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=_SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        httpx_mock.add_response(method="GET", url=_STATUS_URL, json=_STATUS_COMPLETED)
        httpx_mock.add_response(method="GET", url=_RESULTS_URL, json=_RESULTS_COMPLETED)
        await client.execute_query_async(
            "123", "456", ["SELECT 1"], session_id="sess-exec-async"
        )
        body = json.loads(httpx_mock.get_requests()[0].content)
        assert body["sessionId"] == "sess-exec-async"


class TestRefreshMetadataOnSuccess:
    def test_submit_job_sends_refresh_metadata_on_success(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=_SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        client.submit_job(
            "123",
            "456",
            ["SELECT 1"],
            refresh_metadata_on_success=True,
        )
        body = json.loads(httpx_mock.get_requests()[0].content)
        assert body["refreshMetadataOnSuccess"] is True

    def test_submit_job_omits_refresh_metadata_on_success_by_default(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=_SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        client.submit_job("123", "456", ["SELECT 1"])
        body = json.loads(httpx_mock.get_requests()[0].content)
        assert "refreshMetadataOnSuccess" not in body

    async def test_submit_job_async_sends_refresh_metadata_on_success(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=_SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        await client.submit_job_async(
            "123",
            "456",
            ["SELECT 1"],
            refresh_metadata_on_success=True,
        )
        body = json.loads(httpx_mock.get_requests()[0].content)
        assert body["refreshMetadataOnSuccess"] is True

    def test_execute_query_forwards_refresh_metadata_on_success(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=_SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        httpx_mock.add_response(method="GET", url=_STATUS_URL, json=_STATUS_COMPLETED)
        httpx_mock.add_response(method="GET", url=_RESULTS_URL, json=_RESULTS_COMPLETED)
        client.execute_query(
            "123",
            "456",
            ["SELECT 1"],
            refresh_metadata_on_success=True,
        )
        body = json.loads(httpx_mock.get_requests()[0].content)
        assert body["refreshMetadataOnSuccess"] is True

    async def test_execute_query_async_forwards_refresh_metadata_on_success(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        httpx_mock.add_response(
            method="POST",
            url=_SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        httpx_mock.add_response(method="GET", url=_STATUS_URL, json=_STATUS_COMPLETED)
        httpx_mock.add_response(method="GET", url=_RESULTS_URL, json=_RESULTS_COMPLETED)
        await client.execute_query_async(
            "123",
            "456",
            ["SELECT 1"],
            refresh_metadata_on_success=True,
        )
        body = json.loads(httpx_mock.get_requests()[0].content)
        assert body["refreshMetadataOnSuccess"] is True


class TestModels:
    def test_job_state_is_terminal(self):
        assert JobState.COMPLETED.is_terminal()
        assert JobState.FAILED.is_terminal()
        assert JobState.CANCELED.is_terminal()
        assert not JobState.PROCESSING.is_terminal()
        assert not JobState.CREATED.is_terminal()


class TestParseDatetime:
    """Tests for _parse_datetime function (Python 3.10 compatibility)."""

    def test_parse_datetime_none(self):
        from keboola_query_service.models import _parse_datetime

        assert _parse_datetime(None) is None
        assert _parse_datetime("") is None

    def test_parse_datetime_z_suffix(self):
        from datetime import datetime, timezone

        from keboola_query_service.models import _parse_datetime

        result = _parse_datetime("2024-01-15T10:30:45Z")
        assert result == datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)

    def test_parse_datetime_with_timezone(self):
        from datetime import datetime, timezone

        from keboola_query_service.models import _parse_datetime

        result = _parse_datetime("2024-01-15T10:30:45+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)

    def test_parse_datetime_6_digit_fractional_seconds(self):
        from datetime import datetime, timezone

        from keboola_query_service.models import _parse_datetime

        result = _parse_datetime("2024-01-15T10:30:45.123456+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=timezone.utc)

    def test_parse_datetime_5_digit_fractional_seconds(self):
        """Test Python 3.10 compatibility: 5-digit fractional seconds should be padded."""
        from datetime import datetime, timezone

        from keboola_query_service.models import _parse_datetime

        result = _parse_datetime("2024-01-15T10:30:45.12345+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 45, 123450, tzinfo=timezone.utc)

    def test_parse_datetime_more_than_6_digit_fractional_seconds(self):
        """Test Python 3.10 compatibility: >6-digit fractional seconds should be truncated."""
        from datetime import datetime, timezone

        from keboola_query_service.models import _parse_datetime

        result = _parse_datetime("2024-01-15T10:30:45.1234567890+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=timezone.utc)

    def test_parse_datetime_3_digit_fractional_seconds(self):
        """Test Python 3.10 compatibility: 3-digit fractional seconds should be padded."""
        from datetime import datetime, timezone

        from keboola_query_service.models import _parse_datetime

        result = _parse_datetime("2024-01-15T10:30:45.123+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 45, 123000, tzinfo=timezone.utc)

    def test_parse_datetime_z_suffix_with_fractional_seconds(self):
        """Test Z suffix combined with fractional seconds."""
        from datetime import datetime, timezone

        from keboola_query_service.models import _parse_datetime

        result = _parse_datetime("2024-01-15T10:30:45.12345Z")
        assert result == datetime(2024, 1, 15, 10, 30, 45, 123450, tzinfo=timezone.utc)


class TestAutoPagination:
    """Tests for auto-pagination in execute_query / execute_query_async."""

    _SUBMIT_URL = (
        "https://query.test.keboola.com/api/v1/branches/123/workspaces/456/queries"
    )
    _STATUS_URL = "https://query.test.keboola.com/api/v1/queries/job-1"
    _STATUS_COMPLETED = {
        "queryJobId": "job-1",
        "status": "completed",
        "actorType": "user",
        "createdAt": "2024-01-01T00:00:00Z",
        "changedAt": "2024-01-01T00:01:00Z",
        "statements": [{"id": "stmt-1", "query": "SELECT *", "status": "completed"}],
    }

    @staticmethod
    def _results_url(offset: int = 0, page_size: int = 3) -> str:
        return f"https://query.test.keboola.com/api/v1/queries/job-1/stmt-1/results?offset={offset}&pageSize={page_size}"

    @staticmethod
    def _make_page(
        data: list[list[str]],
        number_of_rows: int | None = None,
    ) -> dict:
        page: dict = {
            "status": "completed",
            "columns": [
                {"name": "id", "type": "NUMBER", "nullable": False, "length": 38},
            ],
            "data": data,
        }
        if number_of_rows is not None:
            page["numberOfRows"] = number_of_rows
        return page

    def _setup_submit_and_status(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            method="POST",
            url=self._SUBMIT_URL,
            json={"queryJobId": "job-1"},
            status_code=201,
        )
        httpx_mock.add_response(
            method="GET",
            url=self._STATUS_URL,
            json=self._STATUS_COMPLETED,
        )

    def test_multi_page_exhaustion(self, client: Client, httpx_mock: HTTPXMock) -> None:
        """Results spanning multiple pages are fetched in full."""
        self._setup_submit_and_status(httpx_mock)
        # Page 1: full page (3 rows)
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=0, page_size=3),
            json=self._make_page([["1"], ["2"], ["3"]], number_of_rows=7),
        )
        # Page 2: full page (3 rows)
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=3, page_size=3),
            json=self._make_page([["4"], ["5"], ["6"]], number_of_rows=7),
        )
        # Page 3: partial last page (1 row) — code still requests page_size=3
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=6, page_size=3),
            json=self._make_page([["7"]], number_of_rows=7),
        )

        results = client.execute_query(
            "123",
            "456",
            ["SELECT *"],
            page_size=3,
        )

        assert len(results) == 1
        assert len(results[0].data) == 7
        assert results[0].data == [["1"], ["2"], ["3"], ["4"], ["5"], ["6"], ["7"]]

    def test_number_of_rows_short_circuit(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        """Stops fetching when numberOfRows is reached."""
        self._setup_submit_and_status(httpx_mock)
        # Single full page where numberOfRows == page_size
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=0, page_size=3),
            json=self._make_page([["1"], ["2"], ["3"]], number_of_rows=3),
        )

        results = client.execute_query(
            "123",
            "456",
            ["SELECT *"],
            page_size=3,
        )

        assert len(results[0].data) == 3

    def test_max_rows_cap(self, client: Client, httpx_mock: HTTPXMock) -> None:
        """max_rows caps the total number of rows fetched."""
        self._setup_submit_and_status(httpx_mock)
        # Page 1: 3 rows
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=0, page_size=3),
            json=self._make_page([["1"], ["2"], ["3"]], number_of_rows=10),
        )
        # Page 2: request only 2 more (max_rows=5, already have 3)
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=3, page_size=2),
            json=self._make_page([["4"], ["5"]], number_of_rows=10),
        )

        results = client.execute_query(
            "123",
            "456",
            ["SELECT *"],
            page_size=3,
            max_rows=5,
        )

        assert len(results[0].data) == 5

    def test_empty_result_set(self, client: Client, httpx_mock: HTTPXMock) -> None:
        """Empty result set returns without looping."""
        self._setup_submit_and_status(httpx_mock)
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=0, page_size=3),
            json=self._make_page([], number_of_rows=0),
        )

        results = client.execute_query(
            "123",
            "456",
            ["SELECT *"],
            page_size=3,
        )

        assert len(results[0].data) == 0

    async def test_multi_page_async(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        """Async version auto-paginates correctly."""
        self._setup_submit_and_status(httpx_mock)
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=0, page_size=3),
            json=self._make_page([["1"], ["2"], ["3"]], number_of_rows=5),
        )
        # Page 2: partial page (2 rows) — code requests page_size=3
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=3, page_size=3),
            json=self._make_page([["4"], ["5"]], number_of_rows=5),
        )

        results = await client.execute_query_async(
            "123",
            "456",
            ["SELECT *"],
            page_size=3,
        )

        assert len(results[0].data) == 5

    @pytest.mark.parametrize(
        "page_size,max_rows",
        [
            (0, None),
            (-1, None),
            (10, 0),
            (10, -5),
            (True, None),
            (10, True),
            (False, None),
            (10, False),
        ],
    )
    def test_invalid_pagination_options(
        self, client: Client, page_size: int, max_rows: int | None
    ) -> None:
        """Invalid page_size / max_rows raise ValidationError."""
        with pytest.raises(ValidationError):
            client.execute_query(
                "123",
                "456",
                ["SELECT 1"],
                page_size=page_size,
                max_rows=max_rows,
            )

    @pytest.mark.parametrize(
        "page_size,max_rows",
        [
            (0, None),
            (-1, None),
            (10, 0),
            (10, -5),
            (True, None),
            (10, True),
            (False, None),
            (10, False),
        ],
    )
    async def test_invalid_pagination_options_async(
        self, client: Client, page_size: int, max_rows: int | None
    ) -> None:
        """Async variant also validates pagination options."""
        with pytest.raises(ValidationError):
            await client.execute_query_async(
                "123",
                "456",
                ["SELECT 1"],
                page_size=page_size,
                max_rows=max_rows,
            )

    def test_execute_query_iter_yields_pages(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        """execute_query_iter yields individual pages lazily."""
        self._setup_submit_and_status(httpx_mock)
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=0, page_size=3),
            json=self._make_page([["1"], ["2"], ["3"]], number_of_rows=5),
        )
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=3, page_size=3),
            json=self._make_page([["4"], ["5"]], number_of_rows=5),
        )

        pages = list(client.execute_query_iter("123", "456", ["SELECT *"], page_size=3))

        assert len(pages) == 2
        assert pages[0].data == [["1"], ["2"], ["3"]]
        assert pages[1].data == [["4"], ["5"]]

    def test_execute_query_iter_no_default_max_rows(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        """execute_query_iter defaults to max_rows=None (no cap)."""
        self._setup_submit_and_status(httpx_mock)
        # 7 rows across 3 pages — all fetched because no default cap
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=0, page_size=3),
            json=self._make_page([["1"], ["2"], ["3"]], number_of_rows=7),
        )
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=3, page_size=3),
            json=self._make_page([["4"], ["5"], ["6"]], number_of_rows=7),
        )
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=6, page_size=3),
            json=self._make_page([["7"]], number_of_rows=7),
        )

        pages = list(client.execute_query_iter("123", "456", ["SELECT *"], page_size=3))

        total_rows = sum(len(p.data) for p in pages)
        assert total_rows == 7

    def test_execute_query_iter_with_max_rows(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        """execute_query_iter respects explicit max_rows."""
        self._setup_submit_and_status(httpx_mock)
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=0, page_size=3),
            json=self._make_page([["1"], ["2"], ["3"]], number_of_rows=10),
        )
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=3, page_size=2),
            json=self._make_page([["4"], ["5"]], number_of_rows=10),
        )

        pages = list(
            client.execute_query_iter(
                "123", "456", ["SELECT *"], page_size=3, max_rows=5
            )
        )

        total_rows = sum(len(p.data) for p in pages)
        assert total_rows == 5

    async def test_execute_query_iter_async_yields_pages(
        self, client: Client, httpx_mock: HTTPXMock
    ) -> None:
        """Async iterator variant yields pages lazily."""
        self._setup_submit_and_status(httpx_mock)
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=0, page_size=3),
            json=self._make_page([["1"], ["2"], ["3"]], number_of_rows=5),
        )
        httpx_mock.add_response(
            method="GET",
            url=self._results_url(offset=3, page_size=3),
            json=self._make_page([["4"], ["5"]], number_of_rows=5),
        )

        pages: list = []
        async for page in client.execute_query_iter_async(
            "123", "456", ["SELECT *"], page_size=3
        ):
            pages.append(page)

        assert len(pages) == 2
        assert pages[0].data == [["1"], ["2"], ["3"]]
        assert pages[1].data == [["4"], ["5"]]
