"""Tests for reading the app user token from incoming request headers."""

import httpx
import pytest

from keboola_query_service import APP_USER_TOKEN_HEADER, user_token_from_headers


def test_header_name_matches_apps_proxy_contract():
    assert APP_USER_TOKEN_HEADER == "X-Kbc-App-User-Token"


@pytest.mark.parametrize(
    "name",
    ["X-Kbc-App-User-Token", "x-kbc-app-user-token", "X-KBC-APP-USER-TOKEN"],
)
def test_reads_token_from_dict_with_any_case(name: str):
    assert user_token_from_headers({"Other": "x", name: "user-a"}) == "user-a"


def test_reads_token_from_case_insensitive_mapping():
    headers = httpx.Headers({"x-kbc-app-user-token": "user-a"})

    assert user_token_from_headers(headers) == "user-a"


@pytest.mark.parametrize(
    "headers",
    [{}, {"Other": "x"}, {"X-Kbc-App-User-Token": ""}],
    ids=["empty", "missing", "empty-value"],
)
def test_returns_none_without_token(headers: dict[str, str]):
    assert user_token_from_headers(headers) is None
