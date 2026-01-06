"""Tests for Keboola Query Service Client."""

import pytest
from pytest_httpx import HTTPXMock

from keboola_query_service import (
    Client,
    JobState,
    StatementState,
    AuthenticationError,
    ValidationError,
    JobError,
)


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
                    {"name": "name", "type": "VARCHAR", "nullable": True, "length": 256},
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
            url="https://query.test.keboola.com/api/v1/queries/job-abc123/stmt-1/results?offset=0&pageSize=500",
            json={
                "status": "completed",
                "columns": [{"name": "1", "type": "NUMBER", "nullable": False, "length": 1}],
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
        from keboola_query_service.models import _parse_datetime
        from datetime import datetime, timezone

        result = _parse_datetime("2024-01-15T10:30:45Z")
        assert result == datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)

    def test_parse_datetime_with_timezone(self):
        from keboola_query_service.models import _parse_datetime
        from datetime import datetime, timezone

        result = _parse_datetime("2024-01-15T10:30:45+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)

    def test_parse_datetime_6_digit_fractional_seconds(self):
        from keboola_query_service.models import _parse_datetime
        from datetime import datetime, timezone

        result = _parse_datetime("2024-01-15T10:30:45.123456+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=timezone.utc)

    def test_parse_datetime_5_digit_fractional_seconds(self):
        """Test Python 3.10 compatibility: 5-digit fractional seconds should be padded."""
        from keboola_query_service.models import _parse_datetime
        from datetime import datetime, timezone

        result = _parse_datetime("2024-01-15T10:30:45.12345+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 45, 123450, tzinfo=timezone.utc)

    def test_parse_datetime_more_than_6_digit_fractional_seconds(self):
        """Test Python 3.10 compatibility: >6-digit fractional seconds should be truncated."""
        from keboola_query_service.models import _parse_datetime
        from datetime import datetime, timezone

        result = _parse_datetime("2024-01-15T10:30:45.1234567890+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=timezone.utc)

    def test_parse_datetime_3_digit_fractional_seconds(self):
        """Test Python 3.10 compatibility: 3-digit fractional seconds should be padded."""
        from keboola_query_service.models import _parse_datetime
        from datetime import datetime, timezone

        result = _parse_datetime("2024-01-15T10:30:45.123+00:00")
        assert result == datetime(2024, 1, 15, 10, 30, 45, 123000, tzinfo=timezone.utc)

    def test_parse_datetime_z_suffix_with_fractional_seconds(self):
        """Test Z suffix combined with fractional seconds."""
        from keboola_query_service.models import _parse_datetime
        from datetime import datetime, timezone

        result = _parse_datetime("2024-01-15T10:30:45.12345Z")
        assert result == datetime(2024, 1, 15, 10, 30, 45, 123450, tzinfo=timezone.utc)
