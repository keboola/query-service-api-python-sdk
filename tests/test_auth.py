"""Tests for the credentials the client sends to the Query Service."""

import asyncio
import contextvars
import json
import logging
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from typing import Any

import httpx
import pytest
from pytest_httpx import HTTPXMock

from keboola_query_service import (
    AppUserToken,
    BearerToken,
    Client,
    Credential,
    QueryServiceError,
    StorageToken,
    ValidationError,
)

BASE_URL = "https://query.test.keboola.com"
CREDENTIAL_HEADERS = (
    "X-StorageAPI-Token",
    "Authorization",
    "X-KBC-ProjectId",
    "X-Kbc-App-User-Token",
)

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
    return Client(base_url=BASE_URL, auth=StorageToken("storage-token"))


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


def credential_headers(request: httpx.Request) -> dict[str, str]:
    return {
        name: request.headers[name]
        for name in CREDENTIAL_HEADERS
        if name in request.headers
    }


def sent_credentials(httpx_mock: HTTPXMock) -> list[dict[str, str]]:
    return [credential_headers(request) for request in httpx_mock.get_requests()]


STORAGE_HEADERS = {"X-StorageAPI-Token": "storage-token"}
SERVER_STORAGE = StorageToken("server-token")


def user_headers(user_token: str) -> dict[str, str]:
    return {"X-Kbc-App-User-Token": user_token, "X-StorageAPI-Token": "server-token"}


CREDENTIALS: dict[str, tuple[Credential, dict[str, str]]] = {
    "storage-token": (
        StorageToken("other-storage"),
        {"X-StorageAPI-Token": "other-storage"},
    ),
    "bearer-token": (
        BearerToken("kbc_pat_abc", project_id=123),
        {"Authorization": "Bearer kbc_pat_abc", "X-KBC-ProjectId": "123"},
    ),
    "app-user-token": (
        AppUserToken("user-a", storage_token=SERVER_STORAGE),
        user_headers("user-a"),
    ),
}


class TestCredentialHeaders:
    @pytest.mark.parametrize(
        ("credential", "headers"), CREDENTIALS.values(), ids=CREDENTIALS.keys()
    )
    def test_client_auth_sends_only_its_headers(
        self, query_service: HTTPXMock, credential: Credential, headers: dict[str, str]
    ):
        Client(base_url=BASE_URL, auth=credential).submit_job(**STATEMENTS)

        assert sent_credentials(query_service) == [headers]

    @pytest.mark.parametrize(
        ("credential", "headers"), CREDENTIALS.values(), ids=CREDENTIALS.keys()
    )
    def test_per_call_auth_sends_only_its_headers(
        self,
        client: Client,
        query_service: HTTPXMock,
        credential: Credential,
        headers: dict[str, str],
    ):
        client.submit_job(**STATEMENTS, auth=credential)

        assert sent_credentials(query_service) == [headers]

    def test_shared_client_defaults_carry_no_credential(self, client: Client):
        assert (
            credential_headers(
                httpx.Request("GET", BASE_URL, headers=client._client.headers)
            )
            == {}
        )

    async def test_shared_async_client_defaults_carry_no_credential(
        self, client: Client
    ):
        async_client = client._get_async_client()

        assert (
            credential_headers(
                httpx.Request("GET", BASE_URL, headers=async_client.headers)
            )
            == {}
        )
        await client.close_async()


class TestPerCallAuth:
    @pytest.mark.parametrize("call", SYNC_CALLS.values(), ids=SYNC_CALLS.keys())
    def test_override_on_every_request(
        self, client: Client, query_service: HTTPXMock, call: SyncCall
    ):
        call(client, auth=AppUserToken("user-a", storage_token=SERVER_STORAGE))

        assert sent_credentials(query_service)
        assert all(
            sent == user_headers("user-a") for sent in sent_credentials(query_service)
        )

    @pytest.mark.parametrize("call", ASYNC_CALLS.values(), ids=ASYNC_CALLS.keys())
    async def test_override_on_every_request_async(
        self, client: Client, query_service: HTTPXMock, call: AsyncCall
    ):
        await call(client, auth=AppUserToken("user-a", storage_token=SERVER_STORAGE))
        await client.close_async()

        assert sent_credentials(query_service)
        assert all(
            sent == user_headers("user-a") for sent in sent_credentials(query_service)
        )

    @pytest.mark.parametrize("call", SYNC_CALLS.values(), ids=SYNC_CALLS.keys())
    def test_without_override_uses_client_auth(
        self, client: Client, query_service: HTTPXMock, call: SyncCall
    ):
        call(client)

        assert all(sent == STORAGE_HEADERS for sent in sent_credentials(query_service))

    @pytest.mark.parametrize("call", ASYNC_CALLS.values(), ids=ASYNC_CALLS.keys())
    async def test_without_override_uses_client_auth_async(
        self, client: Client, query_service: HTTPXMock, call: AsyncCall
    ):
        await call(client)
        await client.close_async()

        assert all(sent == STORAGE_HEADERS for sent in sent_credentials(query_service))

    def test_override_does_not_stay_on_client(
        self, client: Client, query_service: HTTPXMock
    ):
        client.submit_job(
            **STATEMENTS, auth=AppUserToken("user-a", storage_token=SERVER_STORAGE)
        )
        client.submit_job(**STATEMENTS)
        client.submit_job(
            **STATEMENTS,
            auth=lambda: AppUserToken("user-b", storage_token=SERVER_STORAGE),
        )

        assert sent_credentials(query_service) == [
            user_headers("user-a"),
            STORAGE_HEADERS,
            user_headers("user-b"),
        ]


class TestProvider:
    def test_called_before_every_request(self, query_service: HTTPXMock):
        calls = 0

        def provider() -> Credential:
            nonlocal calls
            calls += 1
            return StorageToken("storage-token")

        Client(base_url=BASE_URL, auth=provider).execute_query(**STATEMENTS)

        assert calls == len(query_service.get_requests()) == 3

    def test_called_before_every_retry(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(status_code=503, json={"exception": "busy"})
        httpx_mock.add_response(status_code=201, json={"queryJobId": "job-1"})
        credentials = iter(
            [
                AppUserToken("first", storage_token=SERVER_STORAGE),
                AppUserToken("second", storage_token=SERVER_STORAGE),
            ]
        )

        Client(base_url=BASE_URL, auth=lambda: next(credentials)).submit_job(
            **STATEMENTS
        )

        assert sent_credentials(httpx_mock) == [
            user_headers("first"),
            user_headers("second"),
        ]

    async def test_called_before_every_retry_async(self, httpx_mock: HTTPXMock):
        httpx_mock.add_response(status_code=503, json={"exception": "busy"})
        httpx_mock.add_response(status_code=201, json={"queryJobId": "job-1"})
        credentials = iter(
            [
                AppUserToken("first", storage_token=SERVER_STORAGE),
                AppUserToken("second", storage_token=SERVER_STORAGE),
            ]
        )
        client = Client(base_url=BASE_URL, auth=lambda: next(credentials))

        await client.submit_job_async(**STATEMENTS)
        await client.close_async()

        assert sent_credentials(httpx_mock) == [
            user_headers("first"),
            user_headers("second"),
        ]

    def test_switches_users_between_calls(self, query_service: HTTPXMock):
        current = {"user": AppUserToken("user-a", storage_token=SERVER_STORAGE)}
        client = Client(base_url=BASE_URL, auth=lambda: current["user"])

        client.submit_job(**STATEMENTS)
        current["user"] = AppUserToken("user-b", storage_token=SERVER_STORAGE)
        client.submit_job(**STATEMENTS)

        assert sent_credentials(query_service) == [
            user_headers("user-a"),
            user_headers("user-b"),
        ]

    @pytest.mark.parametrize("call", ["stream_results", "stream_results_async"])
    async def test_used_for_streams(self, query_service: HTTPXMock, call: str):
        client = Client(
            base_url=BASE_URL,
            auth=lambda: AppUserToken("user-a", storage_token=SERVER_STORAGE),
        )

        if call == "stream_results":
            SYNC_CALLS[call](client)
        else:
            await ASYNC_CALLS[call](client)
        await client.close_async()

        assert sent_credentials(query_service) == [user_headers("user-a")]

    def test_per_call_provider_wins_over_client_provider(
        self, query_service: HTTPXMock
    ):
        client = Client(
            base_url=BASE_URL,
            auth=lambda: AppUserToken("client", storage_token=SERVER_STORAGE),
        )

        client.submit_job(
            **STATEMENTS,
            auth=lambda: AppUserToken("call", storage_token=SERVER_STORAGE),
        )

        assert sent_credentials(query_service) == [user_headers("call")]

    def test_provider_returning_no_credential_is_an_error(self):
        client = Client(base_url=BASE_URL, auth=lambda: "plain-string")  # type: ignore[arg-type,return-value]

        with pytest.raises(TypeError) as exc_info:
            client.submit_job(**STATEMENTS)

        assert "plain-string" not in str(exc_info.value)


class TestRequestScopedProvider:
    """The README pattern: a ContextVar set per request, one shared client."""

    def _client(self, current: "ContextVar[Credential]") -> Client:
        return Client(base_url=BASE_URL, auth=lambda: current.get())

    def test_threads_each_send_their_request_credential(self, query_service: HTTPXMock):
        server_auth = StorageToken("server-token")
        current: ContextVar[Credential] = ContextVar("current", default=server_auth)
        client = self._client(current)
        users = [f"user-{i}" if i % 3 else "" for i in range(30)]

        def handle_request(user: str) -> None:
            token = current.set(
                AppUserToken(user, storage_token=SERVER_STORAGE)
                if user
                else server_auth
            )
            try:
                client.execute_query(
                    branch_id="123", workspace_id="456", statements=[user]
                )
            finally:
                current.reset(token)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(
                pool.map(
                    lambda u: contextvars.copy_context().run(handle_request, u), users
                )
            )

        submits = [r for r in query_service.get_requests() if r.method == "POST"]
        assert len(submits) == len(users)
        for request in submits:
            user = json.loads(request.content)["statements"][0]
            expected = (
                user_headers(user) if user else {"X-StorageAPI-Token": "server-token"}
            )
            assert credential_headers(request) == expected
        assert current.get() is server_auth

    async def test_tasks_each_send_their_request_credential(
        self, query_service: HTTPXMock
    ):
        server_auth = StorageToken("server-token")
        current: ContextVar[Credential] = ContextVar("current", default=server_auth)
        client = self._client(current)
        users = [f"user-{i}" if i % 3 else "" for i in range(30)]

        async def handle_request(user: str) -> None:
            current.set(
                AppUserToken(user, storage_token=SERVER_STORAGE)
                if user
                else server_auth
            )
            await client.execute_query_async(
                branch_id="123", workspace_id="456", statements=[user]
            )

        await asyncio.gather(*(handle_request(user) for user in users))
        await client.close_async()

        requests = query_service.get_requests()
        assert len(requests) == 3 * len(users)
        for request in [r for r in requests if r.method == "POST"]:
            user = json.loads(request.content)["statements"][0]
            expected = (
                user_headers(user) if user else {"X-StorageAPI-Token": "server-token"}
            )
            assert credential_headers(request) == expected
        assert current.get() is server_auth


CREDENTIAL_TYPES: dict[
    str, tuple[Callable[[Any], Credential], Callable[[str], dict[str, str]]]
] = {
    "storage-token": (
        StorageToken,
        lambda value: {"X-StorageAPI-Token": value},
    ),
    "bearer-token": (
        lambda token: BearerToken(token, project_id=123),
        lambda value: {"Authorization": f"Bearer {value}", "X-KBC-ProjectId": "123"},
    ),
    "app-user-token": (
        lambda token: AppUserToken(token, storage_token=SERVER_STORAGE),
        lambda value: user_headers(value),
    ),
}


class TestTokenCallback:
    @pytest.mark.parametrize(
        ("make", "headers"), CREDENTIAL_TYPES.values(), ids=CREDENTIAL_TYPES.keys()
    )
    def test_value_and_callback_give_the_same_headers(
        self,
        make: Callable[[Any], Credential],
        headers: Callable[[str], dict[str, str]],
    ):
        assert make("tok").credential_headers() == headers("tok")
        assert make(lambda: "tok").credential_headers() == headers("tok")

    def test_called_before_every_request(self, query_service: HTTPXMock):
        calls = 0

        def read_token() -> str:
            nonlocal calls
            calls += 1
            return "storage-token"

        Client(BASE_URL, auth=StorageToken(read_token)).execute_query(**STATEMENTS)

        assert calls == len(query_service.get_requests()) == 3

    @pytest.mark.parametrize("mode", ["sync", "async"])
    async def test_called_before_every_retry(self, httpx_mock: HTTPXMock, mode: str):
        httpx_mock.add_response(status_code=503, json={"exception": "busy"})
        httpx_mock.add_response(status_code=201, json={"queryJobId": "job-1"})
        values = iter(["first", "second"])
        client = Client(BASE_URL, auth=StorageToken(lambda: next(values)))

        if mode == "sync":
            client.submit_job(**STATEMENTS)
        else:
            await client.submit_job_async(**STATEMENTS)
        await client.close_async()

        assert sent_credentials(httpx_mock) == [
            {"X-StorageAPI-Token": "first"},
            {"X-StorageAPI-Token": "second"},
        ]

    def test_rotated_token_is_used_by_the_next_request(self, query_service: HTTPXMock):
        token_file = {"value": "old-token"}
        client = Client(BASE_URL, auth=StorageToken(lambda: token_file["value"]))

        client.submit_job(**STATEMENTS)
        token_file["value"] = "new-token"
        client.submit_job(**STATEMENTS)

        assert sent_credentials(query_service) == [
            {"X-StorageAPI-Token": "old-token"},
            {"X-StorageAPI-Token": "new-token"},
        ]

    @pytest.mark.parametrize(
        "value",
        ["", "sec ret", "secret\n", "seč-secret", 123, None],
        ids=["empty", "space", "newline", "non-ascii", "int", "none"],
    )
    @pytest.mark.parametrize("call", ["submit_job", "stream_results"])
    def test_invalid_callback_value_is_refused_before_sending(
        self, httpx_mock: HTTPXMock, value: object, call: str
    ):
        client = Client(
            BASE_URL, auth=AppUserToken(lambda: value, storage_token=SERVER_STORAGE)
        )  # type: ignore[arg-type,return-value]

        with pytest.raises(ValidationError) as exc_info:
            SYNC_CALLS[call](client)

        assert httpx_mock.get_requests() == []
        assert "sec" not in str(exc_info.value)

    @pytest.mark.parametrize("call", ["submit_job_async", "stream_results_async"])
    async def test_invalid_callback_value_is_refused_before_sending_async(
        self, httpx_mock: HTTPXMock, call: str
    ):
        client = Client(
            BASE_URL, auth=AppUserToken(lambda: "sec ret", storage_token=SERVER_STORAGE)
        )

        with pytest.raises(ValidationError):
            await ASYNC_CALLS[call](client)
        await client.close_async()

        assert httpx_mock.get_requests() == []

    def test_provider_picks_credential_and_callback_gives_its_value(
        self, query_service: HTTPXMock
    ):
        server_token = {"value": "server-1"}
        server_auth = StorageToken(lambda: server_token["value"])
        current: ContextVar[Credential] = ContextVar("current", default=server_auth)
        client = Client(BASE_URL, auth=lambda: current.get())

        client.submit_job(**STATEMENTS)
        server_token["value"] = "server-2"
        current.set(AppUserToken(lambda: "user-a", storage_token=SERVER_STORAGE))
        client.submit_job(**STATEMENTS)
        current.set(server_auth)
        client.submit_job(**STATEMENTS)

        assert sent_credentials(query_service) == [
            {"X-StorageAPI-Token": "server-1"},
            user_headers("user-a"),
            {"X-StorageAPI-Token": "server-2"},
        ]

    @pytest.mark.parametrize("call", SYNC_CALLS.values(), ids=SYNC_CALLS.keys())
    def test_used_on_every_sync_method(self, query_service: HTTPXMock, call: SyncCall):
        call(
            Client(
                BASE_URL,
                auth=AppUserToken(lambda: "user-a", storage_token=SERVER_STORAGE),
            )
        )

        assert sent_credentials(query_service)
        assert all(
            sent == user_headers("user-a") for sent in sent_credentials(query_service)
        )

    @pytest.mark.parametrize("call", ASYNC_CALLS.values(), ids=ASYNC_CALLS.keys())
    async def test_used_on_every_async_method(
        self, query_service: HTTPXMock, call: AsyncCall
    ):
        client = Client(
            BASE_URL, auth=AppUserToken(lambda: "user-a", storage_token=SERVER_STORAGE)
        )

        await call(client)
        await client.close_async()

        assert sent_credentials(query_service)
        assert all(
            sent == user_headers("user-a") for sent in sent_credentials(query_service)
        )

    @pytest.mark.parametrize(
        ("make", "headers"), CREDENTIAL_TYPES.values(), ids=CREDENTIAL_TYPES.keys()
    )
    def test_callback_credential_repr_hides_token(
        self,
        make: Callable[[Any], Credential],
        headers: Callable[[str], dict[str, str]],
    ):
        def read_secret_token() -> str:
            return "secret-token"

        credential = make(read_secret_token)

        assert "secret" not in repr(credential)
        assert "secret" not in str(credential)


class TestAppUserTokenWithStorageToken:
    def test_sends_both_tokens_and_nothing_else(self, query_service: HTTPXMock):
        credential = AppUserToken("user-a", storage_token=StorageToken("app-storage"))

        Client(BASE_URL, auth=credential).submit_job(**STATEMENTS)

        assert credential.credential_headers() == {
            "X-Kbc-App-User-Token": "user-a",
            "X-StorageAPI-Token": "app-storage",
        }
        assert sent_credentials(query_service) == [credential.credential_headers()]

    @pytest.mark.parametrize("mode", ["sync", "async"])
    async def test_both_callbacks_are_read_before_every_retry(
        self, httpx_mock: HTTPXMock, mode: str
    ):
        httpx_mock.add_response(status_code=503, json={"exception": "busy"})
        httpx_mock.add_response(status_code=201, json={"queryJobId": "job-1"})
        users = iter(["user-1", "user-2"])
        storages = iter(["storage-1", "storage-2"])
        credential = AppUserToken(
            lambda: next(users), storage_token=StorageToken(lambda: next(storages))
        )
        client = Client(BASE_URL, auth=credential)

        if mode == "sync":
            client.submit_job(**STATEMENTS)
        else:
            await client.submit_job_async(**STATEMENTS)
        await client.close_async()

        assert sent_credentials(httpx_mock) == [
            {"X-Kbc-App-User-Token": "user-1", "X-StorageAPI-Token": "storage-1"},
            {"X-Kbc-App-User-Token": "user-2", "X-StorageAPI-Token": "storage-2"},
        ]

    def test_both_callbacks_are_read_before_every_request(
        self, query_service: HTTPXMock
    ):
        calls = {"user": 0, "storage": 0}

        def read(name: str) -> Callable[[], str]:
            def reader() -> str:
                calls[name] += 1
                return name

            return reader

        credential = AppUserToken(
            read("user"), storage_token=StorageToken(read("storage"))
        )

        Client(BASE_URL, auth=credential).execute_query(**STATEMENTS)

        assert calls == {"user": 3, "storage": 3}
        assert len(query_service.get_requests()) == 3

    def test_invalid_storage_callback_value_is_refused_before_sending(
        self, httpx_mock: HTTPXMock
    ):
        credential = AppUserToken(
            "user-a", storage_token=StorageToken(lambda: "sec ret")
        )

        with pytest.raises(ValidationError) as exc_info:
            Client(BASE_URL, auth=credential).submit_job(**STATEMENTS)

        assert httpx_mock.get_requests() == []
        assert "sec" not in str(exc_info.value)

    def test_storage_token_is_required(self):
        with pytest.raises(TypeError):
            AppUserToken("user-a")  # type: ignore[call-arg]

    @pytest.mark.parametrize(
        "storage_token",
        [None, "storage-token", BearerToken("kbc_pat_abc", project_id=1)],
        ids=["none", "string", "bearer-token"],
    )
    def test_storage_token_must_be_a_storage_token(self, storage_token: object):
        with pytest.raises(ValidationError):
            AppUserToken("user-a", storage_token=storage_token)  # type: ignore[arg-type]

    def test_repr_hides_both_tokens(self):
        credential = AppUserToken(
            "secret-user", storage_token=StorageToken(lambda: "secret-storage")
        )

        assert "secret" not in repr(credential)
        assert "secret" not in str(credential)


class TestClientArguments:
    def test_token_string_means_storage_token(self, query_service: HTTPXMock):
        Client(BASE_URL, "storage-token").submit_job(**STATEMENTS)
        Client(base_url=BASE_URL, token="storage-token").submit_job(**STATEMENTS)

        assert sent_credentials(query_service) == [STORAGE_HEADERS, STORAGE_HEADERS]

    def test_token_and_auth_is_an_error(self):
        with pytest.raises(ValueError):
            Client(BASE_URL, "storage-token", auth=StorageToken("storage-token"))

    def test_neither_token_nor_auth_is_an_error(self):
        with pytest.raises(ValueError):
            Client(BASE_URL)

    @pytest.mark.parametrize(
        "make",
        [
            lambda: StorageToken(""),
            lambda: BearerToken("", project_id=1),
            lambda: BearerToken("kbc_pat_abc", project_id=0),
            lambda: AppUserToken("", storage_token=SERVER_STORAGE),
        ],
        ids=["empty-storage", "empty-bearer", "bad-project-id", "empty-app-user"],
    )
    def test_invalid_credential_is_an_error(self, make: Callable[[], Credential]):
        with pytest.raises(ValidationError):
            make()


class TestTokenIsNotExposed:
    @pytest.mark.parametrize(
        "credential_type",
        [StorageToken, lambda token: AppUserToken(token, storage_token=SERVER_STORAGE)],
        ids=["storage", "app-user"],
    )
    @pytest.mark.parametrize(
        "token",
        ["secret\n", "sec\rret", "sec\x00ret", "sec ret", "seč-secret"],
        ids=["newline", "carriage-return", "nul", "space", "non-ascii"],
    )
    def test_unsendable_token_is_refused_without_echo(
        self, credential_type: Callable[[str], Credential], token: str
    ):
        with pytest.raises(ValidationError) as exc_info:
            credential_type(token)

        assert "sec" not in str(exc_info.value)

    def test_unsendable_bearer_token_is_refused_without_echo(self):
        with pytest.raises(ValidationError) as exc_info:
            BearerToken("secret\n", project_id=1)

        assert "secret" not in str(exc_info.value)

    @pytest.mark.parametrize(
        "credential",
        [
            StorageToken("secret-token"),
            BearerToken("secret-token", project_id=123),
            AppUserToken("secret-token", storage_token=SERVER_STORAGE),
        ],
        ids=["storage", "bearer", "app-user"],
    )
    def test_not_in_credential_repr(self, credential: Credential):
        assert "secret-token" not in repr(credential)
        assert "secret-token" not in str(credential)

    def test_per_call_token_not_stored_on_client(
        self, client: Client, query_service: HTTPXMock
    ):
        client.submit_job(
            **STATEMENTS,
            auth=AppUserToken("secret-token", storage_token=SERVER_STORAGE),
        )

        assert "secret-token" not in repr(client)
        assert "secret-token" not in repr(vars(client))

    def test_not_logged_on_retry(
        self, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
    ):
        httpx_mock.add_response(status_code=503, json={"exception": "busy"})
        httpx_mock.add_response(status_code=201, json={"queryJobId": "job-1"})
        caplog.set_level(logging.DEBUG)

        Client(
            base_url=BASE_URL,
            auth=AppUserToken("secret-token", storage_token=SERVER_STORAGE),
        ).submit_job(**STATEMENTS)

        assert "Request failed with status 503" in caplog.text
        assert "secret-token" not in caplog.text

    @pytest.mark.parametrize("status_code", [401, 403, 500])
    def test_not_in_error(self, httpx_mock: HTTPXMock, status_code: int):
        client = Client(base_url=BASE_URL, auth=StorageToken("x"), max_retries=0)
        httpx_mock.add_response(status_code=status_code, json={"exception": "failed"})

        with pytest.raises(QueryServiceError) as exc_info:
            client.submit_job(
                **STATEMENTS,
                auth=AppUserToken("secret-token", storage_token=SERVER_STORAGE),
            )

        assert "secret-token" not in str(exc_info.value)
        assert "secret-token" not in repr(exc_info.value)
        assert "secret-token" not in repr(vars(exc_info.value))


class TestConcurrentUsers:
    def test_threads_each_send_only_their_credential(
        self, client: Client, query_service: HTTPXMock
    ):
        users = [f"user-{i}" for i in range(20)]

        def run(user: str) -> None:
            client.submit_job(
                branch_id="123",
                workspace_id="456",
                statements=[user],
                auth=AppUserToken(user, storage_token=SERVER_STORAGE),
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(run, users))

        for request in query_service.get_requests():
            assert credential_headers(request) == user_headers(
                json.loads(request.content)["statements"][0]
            )
        assert len(query_service.get_requests()) == len(users)

    async def test_tasks_each_send_only_their_credential(
        self, client: Client, query_service: HTTPXMock
    ):
        users = [f"user-{i}" for i in range(20)]

        await asyncio.gather(
            *(
                client.submit_job_async(
                    branch_id="123",
                    workspace_id="456",
                    statements=[user],
                    auth=AppUserToken(user, storage_token=SERVER_STORAGE),
                )
                for user in users
            )
        )
        await client.close_async()

        for request in query_service.get_requests():
            assert credential_headers(request) == user_headers(
                json.loads(request.content)["statements"][0]
            )
        assert len(query_service.get_requests()) == len(users)
