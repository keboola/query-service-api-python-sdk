"""Tests for mapping the Query Service row-level security refusal."""

import pytest
from pytest_httpx import HTTPXMock

from keboola_query_service import (
    ROW_LEVEL_SECURITY_DENIED_MESSAGE,
    AppUserToken,
    BearerToken,
    Client,
    Credential,
    QueryServiceError,
    RowLevelSecurityError,
)

SUBMIT_URL = "https://query.test.keboola.com/api/v1/branches/123/workspaces/456/queries"
STREAM_URL = "https://query.test.keboola.com/api/v1/queries/job-1/stmt-1/results/stream"


@pytest.fixture
def client():
    return Client(
        base_url="https://query.test.keboola.com",
        token="test-token",
        max_retries=0,
    )


def submit(client: Client) -> str:
    return client.submit_job(
        branch_id="123", workspace_id="456", statements=["SELECT 1"]
    )


def test_denied_message_matches_query_service():
    assert (
        ROW_LEVEL_SECURITY_DENIED_MESSAGE
        == "Row-level security does not allow this query"
    )


@pytest.mark.parametrize(
    "headers",
    [{}, {"X-KBC-RLS-Mode": "enforce"}],
    ids=["without-rls-mode-header", "with-rls-mode-header"],
)
def test_refusal_raises_row_level_security_error(
    client: Client, httpx_mock: HTTPXMock, headers: dict[str, str]
):
    httpx_mock.add_response(
        method="POST",
        url=SUBMIT_URL,
        status_code=403,
        headers=headers,
        json={
            "error": 403,
            "exception": ROW_LEVEL_SECURITY_DENIED_MESSAGE,
            "exceptionId": "exc-1",
            "status": "error",
        },
    )

    with pytest.raises(RowLevelSecurityError) as exc_info:
        submit(client)

    assert isinstance(exc_info.value, QueryServiceError)
    assert exc_info.value.status_code == 403
    assert exc_info.value.message == ROW_LEVEL_SECURITY_DENIED_MESSAGE
    assert exc_info.value.exception_id == "exc-1"


async def test_refusal_raises_row_level_security_error_async(
    client: Client, httpx_mock: HTTPXMock
):
    httpx_mock.add_response(
        method="POST",
        url=SUBMIT_URL,
        status_code=403,
        json={"exception": ROW_LEVEL_SECURITY_DENIED_MESSAGE},
    )

    with pytest.raises(RowLevelSecurityError):
        await client.submit_job_async(
            branch_id="123", workspace_id="456", statements=["SELECT 1"]
        )
    await client.close_async()


def test_refusal_on_stream_raises_row_level_security_error(
    client: Client, httpx_mock: HTTPXMock
):
    httpx_mock.add_response(
        method="GET",
        url=STREAM_URL,
        status_code=403,
        json={"exception": ROW_LEVEL_SECURITY_DENIED_MESSAGE},
    )

    with pytest.raises(RowLevelSecurityError):
        list(client.stream_results("job-1", "stmt-1"))


@pytest.mark.parametrize(
    ("status_code", "headers", "body"),
    [
        pytest.param(
            403,
            {"X-KBC-RLS-Mode": "enforce"},
            {"exception": "Failed to get workspace credentials"},
            id="credentials-403-on-governed-workspace",
        ),
        pytest.param(
            403,
            {},
            {"exception": "Failed to get workspace credentials"},
            id="credentials-403-without-header",
        ),
        pytest.param(
            409,
            {},
            {"exception": ROW_LEVEL_SECURITY_DENIED_MESSAGE},
            id="denied-text-on-other-status",
        ),
        pytest.param(
            503,
            {"X-KBC-RLS-Mode": "enforce"},
            {"exception": "Row-level security could not be applied to this workspace"},
            id="rls-unavailable-503",
        ),
    ],
)
def test_other_errors_stay_plain_query_service_error(
    client: Client,
    httpx_mock: HTTPXMock,
    status_code: int,
    headers: dict[str, str],
    body: dict[str, str],
):
    httpx_mock.add_response(
        method="POST",
        url=SUBMIT_URL,
        status_code=status_code,
        headers=headers,
        json=body,
    )

    with pytest.raises(QueryServiceError) as exc_info:
        submit(client)

    assert type(exc_info.value) is QueryServiceError


def test_non_json_body_with_denied_text_stays_plain_query_service_error(
    client: Client, httpx_mock: HTTPXMock
):
    httpx_mock.add_response(
        method="POST",
        url=SUBMIT_URL,
        status_code=403,
        headers={"X-KBC-RLS-Mode": "enforce"},
        text=ROW_LEVEL_SECURITY_DENIED_MESSAGE,
    )

    with pytest.raises(QueryServiceError) as exc_info:
        submit(client)

    assert type(exc_info.value) is QueryServiceError


@pytest.mark.parametrize(
    "auth",
    [None, AppUserToken("user-a"), BearerToken("kbc_pat_abc", project_id=1)],
    ids=["client-storage-token", "app-user-token", "bearer-token"],
)
def test_refusal_does_not_depend_on_auth(
    client: Client, httpx_mock: HTTPXMock, auth: Credential | None
):
    httpx_mock.add_response(
        method="POST",
        url=SUBMIT_URL,
        status_code=403,
        json={"exception": ROW_LEVEL_SECURITY_DENIED_MESSAGE},
    )

    with pytest.raises(RowLevelSecurityError):
        client.submit_job(
            branch_id="123",
            workspace_id="456",
            statements=["SELECT 1"],
            auth=auth,
        )


async def test_refusal_on_stream_raises_row_level_security_error_async(
    client: Client, httpx_mock: HTTPXMock
):
    httpx_mock.add_response(
        method="GET",
        url=STREAM_URL,
        status_code=403,
        json={"exception": ROW_LEVEL_SECURITY_DENIED_MESSAGE},
    )

    with pytest.raises(RowLevelSecurityError):
        async for _ in client.stream_results_async("job-1", "stmt-1"):
            pass
    await client.close_async()
