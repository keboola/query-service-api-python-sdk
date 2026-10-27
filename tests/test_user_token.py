"""Tests for forwarding the app user token to the Query Service."""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx
import pytest
from pytest_httpx import HTTPXMock

from keboola_query_service import Client, QueryServiceError

BASE_URL = "https://query.test.keboola.com"
HEADER = "X-Kbc-App-User-Token"

JOB_STATUS = {
    "queryJobId": "job-1",
    "status": "completed",
    "actorType": "user",
    "createdAt": "2024-01-01T00:00:00Z",
    "changedAt": "2024-01-01T00:01:00Z",
    "statements": [{"id": "stmt-1", "query": "SELECT 1", "status": "completed"}],
}
RESULT_PAGE = {
    "status": "completed",
    "columns": [{"name": "1", "type": "NUMBER", "nullable": False}],
    "data": [["1"]],
    "numberOfRows": 1,
}


def fake_query_service(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path.endswith("/workspaces/456/queries"):
        if request.method == "POST":
            return httpx.Response(201, json={"queryJobId": "job-1"})
        return httpx.Response(200, json={"statements": []})
    if path.endswith("/cancel"):
        return httpx.Response(200, json={"queryJobId": "job-1"})
    if path.endswith("/results/stream"):
        return httpx.Response(200, text='{"a": 1}\n')
    if path.endswith("/results"):
        return httpx.Response(200, json=RESULT_PAGE)
    return httpx.Response(200, json=JOB_STATUS)


@pytest.fixture
def query_service(httpx_mock: HTTPXMock) -> HTTPXMock:
    httpx_mock.add_callback(fake_query_service, is_reusable=True)
    return httpx_mock


@pytest.fixture
def client() -> Client:
    return Client(base_url=BASE_URL, token="test-token")


SyncCall = Callable[..., Any]
AsyncCall = Callable[..., Awaitable[Any]]

STATEMENTS = {"branch_id": "123", "workspace_id": "456", "statements": ["SELECT 1"]}
WORKSPACE = {"branch_id": "123", "workspace_id": "456"}

SYNC_CALLS: dict[str, SyncCall] = {
    "submit_job": lambda c, **kw: c.submit_job(**STATEMENTS, **kw),
    "get_job_status": lambda c, **kw: c.get_job_status("job-1", **kw),
    "get_job_results": lambda c, **kw: c.get_job_results("job-1", "stmt-1", **kw),
    "cancel_job": lambda c, **kw: c.cancel_job("job-1", **kw),
    "get_query_history": lambda c, **kw: c.get_query_history(**WORKSPACE, **kw),
    "wait_for_job": lambda c, **kw: c.wait_for_job("job-1", **kw),
    "execute_query": lambda c, **kw: c.execute_query(**STATEMENTS, **kw),
    "execute_query_iter": lambda c, **kw: list(
        c.execute_query_iter(**STATEMENTS, **kw)
    ),
    "stream_results": lambda c, **kw: list(c.stream_results("job-1", "stmt-1", **kw)),
}


async def _collect(iterator: Any) -> list[Any]:
    return [item async for item in iterator]


ASYNC_CALLS: dict[str, AsyncCall] = {
    "submit_job_async": lambda c, **kw: c.submit_job_async(**STATEMENTS, **kw),
    "get_job_status_async": lambda c, **kw: c.get_job_status_async("job-1", **kw),
    "get_job_results_async": lambda c, **kw: c.get_job_results_async(
        "job-1", "stmt-1", **kw
    ),
    "cancel_job_async": lambda c, **kw: c.cancel_job_async("job-1", **kw),
    "get_query_history_async": lambda c, **kw: c.get_query_history_async(
        **WORKSPACE, **kw
    ),
    "wait_for_job_async": lambda c, **kw: c.wait_for_job_async("job-1", **kw),
    "execute_query_async": lambda c, **kw: c.execute_query_async(**STATEMENTS, **kw),
    "execute_query_iter_async": lambda c, **kw: _collect(
        c.execute_query_iter_async(**STATEMENTS, **kw)
    ),
    "stream_results_async": lambda c, **kw: _collect(
        c.stream_results_async("job-1", "stmt-1", **kw)
    ),
}


def sent_tokens(httpx_mock: HTTPXMock) -> list[str | None]:
    return [request.headers.get(HEADER) for request in httpx_mock.get_requests()]


class TestPerCallUserToken:
    @pytest.mark.parametrize("call", SYNC_CALLS.values(), ids=SYNC_CALLS.keys())
    def test_sends_token_on_every_request(
        self, client: Client, query_service: HTTPXMock, call: SyncCall
    ):
        call(client, user_token="user-a")

        assert sent_tokens(query_service)
        assert set(sent_tokens(query_service)) == {"user-a"}

    @pytest.mark.parametrize("call", ASYNC_CALLS.values(), ids=ASYNC_CALLS.keys())
    async def test_sends_token_on_every_request_async(
        self, client: Client, query_service: HTTPXMock, call: AsyncCall
    ):
        await call(client, user_token="user-a")
        await client.close_async()

        assert sent_tokens(query_service)
        assert set(sent_tokens(query_service)) == {"user-a"}

    @pytest.mark.parametrize("call", SYNC_CALLS.values(), ids=SYNC_CALLS.keys())
    def test_without_token_sends_no_header(
        self, client: Client, query_service: HTTPXMock, call: SyncCall
    ):
        call(client)

        assert set(sent_tokens(query_service)) == {None}

    def test_execute_query_sends_token_on_submit_poll_and_results(
        self, client: Client, query_service: HTTPXMock
    ):
        client.execute_query(**STATEMENTS, user_token="user-a")

        requests = query_service.get_requests()
        assert [r.method for r in requests] == ["POST", "GET", "GET"]
        assert all(r.headers[HEADER] == "user-a" for r in requests)

    def test_token_does_not_stay_on_shared_client(
        self, client: Client, query_service: HTTPXMock
    ):
        client.submit_job(**STATEMENTS, user_token="user-a")
        client.submit_job(**STATEMENTS)
        client.submit_job(**STATEMENTS, user_token="user-b")

        assert sent_tokens(query_service) == ["user-a", None, "user-b"]

    def test_keeps_storage_token(self, client: Client, query_service: HTTPXMock):
        client.submit_job(**STATEMENTS, user_token="user-a")

        request = query_service.get_requests()[0]
        assert request.headers["X-StorageAPI-Token"] == "test-token"

    def test_empty_token_sends_no_header(
        self, client: Client, query_service: HTTPXMock
    ):
        client.submit_job(**STATEMENTS, user_token="")

        assert sent_tokens(query_service) == [None]


class TestTokenIsNotExposed:
    def test_token_not_stored_on_client(self, client: Client, query_service: HTTPXMock):
        client.submit_job(**STATEMENTS, user_token="secret-user-token")

        assert "secret-user-token" not in repr(client)
        assert "secret-user-token" not in repr(vars(client))

    def test_token_not_logged_on_retry(
        self, client: Client, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
    ):
        httpx_mock.add_response(status_code=503, json={"exception": "busy"})
        httpx_mock.add_response(status_code=201, json={"queryJobId": "job-1"})
        caplog.set_level(logging.DEBUG)

        client.submit_job(**STATEMENTS, user_token="secret-user-token")

        assert "Request failed with status 503" in caplog.text
        assert "secret-user-token" not in caplog.text

    @pytest.mark.parametrize("status_code", [401, 403, 500])
    def test_token_not_in_error(self, httpx_mock: HTTPXMock, status_code: int):
        client = Client(base_url=BASE_URL, token="test-token", max_retries=0)
        httpx_mock.add_response(
            status_code=status_code,
            json={"exception": "Row-level security does not allow this query"},
        )

        with pytest.raises(QueryServiceError) as exc_info:
            client.submit_job(**STATEMENTS, user_token="secret-user-token")

        assert "secret-user-token" not in str(exc_info.value)
        assert "secret-user-token" not in repr(exc_info.value)
        assert "secret-user-token" not in repr(vars(exc_info.value))


class TestConcurrentUsers:
    def test_threads_each_send_only_their_token(
        self, client: Client, query_service: HTTPXMock
    ):
        users = [f"user-{i}" for i in range(20)]

        def run(user: str) -> None:
            client.submit_job(
                branch_id="123", workspace_id="456", statements=[user], user_token=user
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(run, users))

        for request in query_service.get_requests():
            assert [request.headers[HEADER]] == json.loads(request.content)[
                "statements"
            ]
        assert len(query_service.get_requests()) == len(users)

    async def test_tasks_each_send_only_their_token(
        self, client: Client, query_service: HTTPXMock
    ):
        users = [f"user-{i}" for i in range(20)]

        await asyncio.gather(
            *(
                client.submit_job_async(
                    branch_id="123",
                    workspace_id="456",
                    statements=[user],
                    user_token=user,
                )
                for user in users
            )
        )
        await client.close_async()

        for request in query_service.get_requests():
            assert [request.headers[HEADER]] == json.loads(request.content)[
                "statements"
            ]
        assert len(query_service.get_requests()) == len(users)
