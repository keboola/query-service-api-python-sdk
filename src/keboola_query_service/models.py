"""Data models for Keboola Query Service SDK."""

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ActorType(str, Enum):
    """Actor type for query jobs."""

    USER = "user"
    SYSTEM = "system"


class JobState(str, Enum):
    """State of a query job."""

    CREATED = "created"
    ENQUEUED = "enqueued"
    PROCESSING = "processing"
    CANCELED = "canceled"
    COMPLETED = "completed"
    FAILED = "failed"

    def is_terminal(self) -> bool:
        """Check if this is a terminal state."""
        return self in (JobState.COMPLETED, JobState.FAILED, JobState.CANCELED)


class StatementState(str, Enum):
    """State of a statement within a job."""

    WAITING = "waiting"
    PROCESSING = "processing"
    CANCELED = "canceled"
    COMPLETED = "completed"
    FAILED = "failed"
    NOT_EXECUTED = "notExecuted"


@dataclass
class Column:
    """Column metadata from query results."""

    name: str
    type: str
    nullable: bool
    length: int | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Column":
        return cls(
            name=data["name"],
            type=data["type"],
            nullable=data["nullable"],
            length=data.get("length"),
        )


@dataclass
class Statement:
    """A SQL statement within a query job."""

    id: str
    query: str
    status: StatementState
    query_id: str | None = None
    session_id: str | None = None
    error: str | None = None
    rows_affected: int | None = None
    number_of_rows: int | None = None
    created_at: datetime | None = None
    executed_at: datetime | None = None
    completed_at: datetime | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Statement":
        return cls(
            id=data["id"],
            query=data["query"],
            status=StatementState(data["status"]),
            query_id=data.get("queryId"),
            session_id=data.get("sessionId"),
            error=data.get("error"),
            rows_affected=data.get("rowsAffected"),
            number_of_rows=data.get("numberOfRows"),
            created_at=_parse_datetime(data.get("createdAt")),
            executed_at=_parse_datetime(data.get("executedAt")),
            completed_at=_parse_datetime(data.get("completedAt")),
        )


@dataclass
class JobStatus:
    """Status of a query job."""

    query_job_id: str
    status: JobState
    actor_type: ActorType
    statements: list[Statement]
    created_at: datetime
    changed_at: datetime
    canceled_at: datetime | None = None
    cancellation_reason: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobStatus":
        return cls(
            query_job_id=data["queryJobId"],
            status=JobState(data["status"]),
            actor_type=ActorType(data["actorType"]),
            statements=[Statement.from_dict(s) for s in data["statements"]],
            created_at=_parse_datetime(data["createdAt"]),  # type: ignore
            changed_at=_parse_datetime(data["changedAt"]),  # type: ignore
            canceled_at=_parse_datetime(data.get("canceledAt")),
            cancellation_reason=data.get("cancellationReason"),
        )

    def get_failed_statements(self) -> list[Statement]:
        """Get all failed statements."""
        return [s for s in self.statements if s.status == StatementState.FAILED]

    def get_first_error(self) -> str | None:
        """Get error message from first failed statement."""
        failed = self.get_failed_statements()
        return failed[0].error if failed else None


@dataclass
class QueryResult:
    """Result of a query statement."""

    status: StatementState
    columns: list[Column] = field(default_factory=list)
    data: list[list[Any]] = field(default_factory=list)
    rows_affected: int | None = None
    number_of_rows: int | None = None
    message: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueryResult":
        return cls(
            status=StatementState(data["status"]),
            columns=[Column.from_dict(c) for c in data.get("columns", [])],
            data=data.get("data", []),
            rows_affected=data.get("rowsAffected"),
            number_of_rows=data.get("numberOfRows"),
            message=data.get("message"),
        )


@dataclass
class StatementWithWorkspaceInfo(Statement):
    """Statement with additional workspace info for query history."""

    query_job_id: str = ""
    warehouse: str | None = None
    backend_size: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StatementWithWorkspaceInfo":
        base = Statement.from_dict(data)
        return cls(
            id=base.id,
            query=base.query,
            status=base.status,
            query_id=base.query_id,
            session_id=base.session_id,
            error=base.error,
            rows_affected=base.rows_affected,
            number_of_rows=base.number_of_rows,
            created_at=base.created_at,
            executed_at=base.executed_at,
            completed_at=base.completed_at,
            query_job_id=data["queryJobId"],
            warehouse=data.get("warehouse"),
            backend_size=data.get("backendSize"),
        )


@dataclass
class QueryHistory:
    """Query history response."""

    statements: list[StatementWithWorkspaceInfo]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "QueryHistory":
        return cls(
            statements=[
                StatementWithWorkspaceInfo.from_dict(s) for s in data["statements"]
            ]
        )


def _parse_datetime(value: str | None) -> datetime | None:
    """Parse ISO datetime string."""
    if not value:
        return None
    # Handle ISO format with Z suffix
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    # Normalize fractional seconds to 6 digits for Python 3.10 compatibility
    # Python 3.10's fromisoformat() doesn't handle arbitrary precision
    match = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)(.*)$", value)
    if match:
        base, frac, suffix = match.groups()
        # Pad or truncate to exactly 6 digits (microseconds)
        frac = frac[:6].ljust(6, "0")
        value = f"{base}.{frac}{suffix}"
    return datetime.fromisoformat(value)
