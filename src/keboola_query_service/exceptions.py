"""Exceptions for Keboola Query Service SDK."""

from typing import Any


class QueryServiceError(Exception):
    """Base exception for Query Service errors."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        exception_id: str | None = None,
        context: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.exception_id = exception_id
        self.context = context or {}

    def __str__(self) -> str:
        parts = [self.message]
        if self.status_code:
            parts.append(f"(status: {self.status_code})")
        if self.exception_id:
            parts.append(f"[{self.exception_id}]")
        return " ".join(parts)


class AuthenticationError(QueryServiceError):
    """Raised when authentication fails (401)."""

    pass


class ValidationError(QueryServiceError):
    """Raised when the SDK or the server refuses the input of a request.

    The SDK refuses it before sending anything (``status_code`` is None), for
    example a bad ``page_size`` or an unusable credential (``CredentialError``).
    The server refuses it with a 400 (``status_code`` is 400).
    """

    pass


class CredentialError(ValidationError):
    """Raised before anything is sent when a credential cannot be used.

    An empty or unsendable token, a token callback that returns one, a bad
    ``project_id`` or a wrong ``storage_token``. It has no status code, as no
    request was made. Catch it before ``ValidationError`` to tell it apart from
    other invalid input.
    """

    pass


class NotFoundError(QueryServiceError):
    """Raised when resource is not found (404)."""

    pass


class JobError(QueryServiceError):
    """Raised when a query job fails."""

    def __init__(
        self,
        message: str,
        job_id: str,
        failed_statements: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ):
        super().__init__(message, **kwargs)
        self.job_id = job_id
        self.failed_statements = failed_statements or []


class JobTimeoutError(QueryServiceError):
    """Raised when waiting for job completion times out."""

    def __init__(self, message: str, job_id: str, **kwargs: Any):
        super().__init__(message, **kwargs)
        self.job_id = job_id


ROW_LEVEL_SECURITY_DENIED_MESSAGE = "Row-level security does not allow this query"


class RowLevelSecurityError(QueryServiceError):
    """Raised when row-level security refuses a query on a governed workspace (403).

    A governed workspace refuses every query whose caller matches no policy, also
    a call with only the Storage API token. The refusal does not say whether a
    policy is missing or the access is denied.

    Query Service refuses only some callers this way. An administrator (a Storage
    API token, as in the MCP server or kbagent) is refused when a table has no
    policy or no rule selects the caller. An app user (an app user token) is not:
    for such a table, Query Service answers a successful result with no rows, so
    an empty result of an app user can also mean "no access". The error is also
    raised for statements Query Service does not allow in a governed workspace
    (for example ``SHOW`` or ``INFORMATION_SCHEMA``) and for a table that a
    column policy withholds from the caller.
    """

    pass
