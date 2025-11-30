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
    """Raised when request validation fails (400)."""

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
