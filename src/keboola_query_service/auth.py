"""Credentials for Keboola Query Service requests."""

from collections.abc import Callable
from dataclasses import dataclass, field


def _require_token(token: str) -> None:
    if not isinstance(token, str) or not token:
        raise ValueError("token must be a non-empty string")
    # httpx puts an unsendable header value into its error message.
    if not (token.isascii() and token.isprintable()) or " " in token:
        raise ValueError(
            "token must contain only printable ASCII characters without spaces"
        )


@dataclass(frozen=True)
class StorageToken:
    """Keboola Storage API token, sent as ``X-StorageAPI-Token``."""

    token: str = field(repr=False)

    def __post_init__(self) -> None:
        _require_token(self.token)

    def _headers(self) -> dict[str, str]:
        return {"X-StorageAPI-Token": self.token}


@dataclass(frozen=True)
class BearerToken:
    """Programmatic token (``kbc_pat_``/``kbc_at_``) for one project.

    Sent as ``Authorization: Bearer <token>`` with ``X-KBC-ProjectId``.
    """

    token: str = field(repr=False)
    project_id: int

    def __post_init__(self) -> None:
        _require_token(self.token)
        if (
            isinstance(self.project_id, bool)
            or not isinstance(self.project_id, int)
            or self.project_id <= 0
        ):
            raise ValueError("project_id must be a positive integer")

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-KBC-ProjectId": str(self.project_id),
        }


@dataclass(frozen=True)
class AppUserToken:
    """App user token of a data app user, sent as ``X-Kbc-App-User-Token``."""

    token: str = field(repr=False)

    def __post_init__(self) -> None:
        _require_token(self.token)

    def _headers(self) -> dict[str, str]:
        return {"X-Kbc-App-User-Token": self.token}


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
    return credential._headers()
