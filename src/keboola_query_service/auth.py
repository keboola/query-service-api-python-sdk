"""Credentials for Keboola Query Service requests."""

from collections.abc import Callable
from typing import Any, NoReturn

from .exceptions import ValidationError

TokenSource = str | Callable[[], str]
"""A token value, or a callable that returns the current one."""


def _require_token(token: object) -> str:
    # httpx puts an unsendable header value into its error message.
    if (
        not isinstance(token, str)
        or not token
        or not (token.isascii() and token.isprintable())
        or " " in token
    ):
        raise ValidationError("token must be non-empty printable ASCII without spaces")
    return token


class _TokenValue:
    """Checks a fixed token at once and a callable's token each time it is read."""

    __slots__ = ("_source",)

    def __init__(self, source: TokenSource) -> None:
        self._source = source if callable(source) else _require_token(source)

    def read(self) -> str:
        source = self._source
        return _require_token(source()) if callable(source) else source

    def __repr__(self) -> str:
        return "<token>"


class _Credential:
    __slots__ = ("_token",)
    _token: _TokenValue

    def __init__(self, token: TokenSource) -> None:
        object.__setattr__(self, "_token", _TokenValue(token))

    def __setattr__(self, name: str, value: Any) -> NoReturn:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __repr__(self) -> str:
        return f"{type(self).__name__}()"


class StorageToken(_Credential):
    """Keboola Storage API token, sent as ``X-StorageAPI-Token``."""

    __slots__ = ()

    def credential_headers(self) -> dict[str, str]:
        """Return the headers of this credential, with the current token."""
        return {"X-StorageAPI-Token": self._token.read()}


class BearerToken(_Credential):
    """Programmatic token (``kbc_pat_``/``kbc_at_``) for one project.

    Sent as ``Authorization: Bearer <token>`` with ``X-KBC-ProjectId``.
    """

    __slots__ = ("project_id",)
    project_id: int

    def __init__(self, token: TokenSource, project_id: int) -> None:
        if (
            isinstance(project_id, bool)
            or not isinstance(project_id, int)
            or project_id <= 0
        ):
            raise ValidationError("project_id must be a positive integer")
        super().__init__(token)
        object.__setattr__(self, "project_id", project_id)

    def credential_headers(self) -> dict[str, str]:
        """Return the headers of this credential, with the current token."""
        return {
            "Authorization": f"Bearer {self._token.read()}",
            "X-KBC-ProjectId": str(self.project_id),
        }

    def __repr__(self) -> str:
        return f"BearerToken(project_id={self.project_id})"


class AppUserToken(_Credential):
    """App user token of a data app user, sent with the app's Storage API token.

    Sent as ``X-Kbc-App-User-Token`` and ``X-StorageAPI-Token``.
    """

    __slots__ = ("storage_token",)
    storage_token: StorageToken

    def __init__(self, token: TokenSource, *, storage_token: StorageToken) -> None:
        if not isinstance(storage_token, StorageToken):
            raise ValidationError("storage_token must be a StorageToken")
        super().__init__(token)
        object.__setattr__(self, "storage_token", storage_token)

    def credential_headers(self) -> dict[str, str]:
        """Return the headers of this credential, with the current tokens."""
        return {
            "X-Kbc-App-User-Token": self._token.read(),
            **self.storage_token.credential_headers(),
        }


Credential = StorageToken | BearerToken | AppUserToken
CredentialProvider = Callable[[], Credential]
Auth = Credential | CredentialProvider


def _resolve_headers(auth: Auth) -> dict[str, str]:
    credential = auth if isinstance(auth, Credential) else auth()
    if not isinstance(credential, Credential):
        raise TypeError(
            "auth must be StorageToken, BearerToken, AppUserToken "
            "or a callable that returns one"
        )
    return credential.credential_headers()
