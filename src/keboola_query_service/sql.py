"""SQL escape helper for the Keboola Query Service.

Provides safe value interpolation into raw SQL strings for Snowflake and
BigQuery. See docs/superpowers/specs/2026-04-21-sdk-quote-helper-design.md
in connection-docs for the design rationale.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Dialect = Literal["snowflake", "bigquery"]

_VALID_DIALECTS: tuple[Dialect, ...] = ("snowflake", "bigquery")


@dataclass(frozen=True)
class SafeSql:
    """Trust marker for already-escaped SQL fragments.

    Do not construct directly for user input — use ``SQL.literal()``,
    ``SQL.ident()``, ``SQL.date()``, or ``SQL.raw()``.
    """

    sql: str

    def __str__(self) -> str:
        return self.sql


class SQL:
    """Dialect-bound SQL escape helper."""

    def __init__(self, dialect: Dialect) -> None:
        if dialect not in _VALID_DIALECTS:
            raise ValueError(
                f"Unknown dialect: {dialect!r}. "
                f"Supported: 'snowflake', 'bigquery'"
            )
        self.dialect: Dialect = dialect
