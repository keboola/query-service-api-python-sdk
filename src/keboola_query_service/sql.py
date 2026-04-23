"""SQL escape helper for the Keboola Query Service.

Dialect-bound factory producing safe SQL fragments. Callers declare the
SQL type of every literal explicitly via the ``type=`` keyword argument.
See docs/superpowers/specs/2026-04-21-sdk-quote-helper-design.md for the
design rationale.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

Dialect = Literal["snowflake", "bigquery"]

_VALID_DIALECTS: tuple[Dialect, ...] = ("snowflake", "bigquery")


@dataclass(frozen=True)
class SafeSql:
    """Trust marker for already-escaped SQL fragments.

    Do not construct directly for user input — use ``SQL.literal()``,
    ``SQL.ident()``, ``SQL.list()``, or ``SQL.raw()``.
    """

    sql: str

    def __str__(self) -> str:
        return self.sql


# Per-dialect dispatch entry: (aliases, accepted_types_label, emitter).
_DispatchEntry = tuple[tuple[str, ...], str, Callable[[object], str]]


def _emit_string(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError(
            f"literal(type='STRING') expects str, got {type(value).__name__}: {value!r}"
        )
    if "\x00" in value:
        raise ValueError(
            "String literal contains NUL character, which neither "
            "Snowflake nor BigQuery accept"
        )
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return "'" + escaped + "'"


_SNOWFLAKE_TYPES: dict[str, _DispatchEntry] = {
    "STRING": (("VARCHAR", "CHAR", "CHARACTER", "TEXT"), "str", _emit_string),
}

_BIGQUERY_TYPES: dict[str, _DispatchEntry] = {
    "STRING": ((), "str", _emit_string),
}


def _build_alias_index(table: dict[str, _DispatchEntry]) -> dict[str, str]:
    """Flatten canonical + aliases → canonical lookup, all upper-cased."""
    index: dict[str, str] = {}
    for canonical, (aliases, _, _) in table.items():
        index[canonical.upper()] = canonical
        for alias in aliases:
            index[alias.upper()] = canonical
    return index


_SNOWFLAKE_ALIAS_INDEX = _build_alias_index(_SNOWFLAKE_TYPES)
_BIGQUERY_ALIAS_INDEX = _build_alias_index(_BIGQUERY_TYPES)


class SQL:
    """Dialect-bound SQL escape helper.

    All ``literal()`` calls require an explicit ``type=`` keyword argument
    naming the SQL type. Type names are case-insensitive. Aliases emit
    identically to their canonical form (e.g. ``VARCHAR`` == ``STRING``).
    """

    def __init__(self, dialect: Dialect) -> None:
        if dialect not in _VALID_DIALECTS:
            raise ValueError(
                f"Unknown dialect: {dialect!r}. "
                f"Supported: 'snowflake', 'bigquery'"
            )
        self.dialect: Dialect = dialect
        if dialect == "snowflake":
            self._types = _SNOWFLAKE_TYPES
            self._alias_index = _SNOWFLAKE_ALIAS_INDEX
        else:
            self._types = _BIGQUERY_TYPES
            self._alias_index = _BIGQUERY_ALIAS_INDEX

    def literal(self, value: object, *, type: str) -> SafeSql:
        """Escape ``value`` into a SQL literal of the declared ``type``."""
        canonical = self._resolve_type(type)
        if value is None:
            return SafeSql(sql="NULL")
        _, _expected, emitter = self._types[canonical]
        return SafeSql(sql=emitter(value))

    def _resolve_type(self, type_name: str) -> str:
        key = type_name.upper()
        if key not in self._alias_index:
            supported = ", ".join(sorted(self._types.keys()))
            raise ValueError(
                f"Unknown SQL type {type_name!r} for dialect "
                f"{self.dialect!r}. Supported: {supported}"
            )
        return self._alias_index[key]

    def raw(self, s: str) -> SafeSql:
        """Wrap a string as a pre-escaped ``SafeSql`` fragment.

        Escape hatch for SQL the helper doesn't escape directly
        (``CURRENT_TIMESTAMP``, backend-specific function calls). Use
        only with strings you fully control — never user input.
        """
        if not isinstance(s, str):
            raise TypeError(f"raw() requires str, got: {type(s).__name__}")
        return SafeSql(sql=s)

    def ident(self, *parts: str) -> SafeSql:
        """Quote one or more identifier parts and join with dots.

        Dots inside a part are preserved (never split). Each part is
        quoted and escaped per the active dialect.
        """
        if not parts:
            raise ValueError("ident() requires at least one part")
        escaped = [self._quote_ident_part(p) for p in parts]
        return SafeSql(sql=".".join(escaped))

    def _quote_ident_part(self, part: object) -> str:
        if not isinstance(part, str):
            raise TypeError(
                f"ident() part must be a non-empty string, got: {part!r}"
            )
        if part == "":
            raise ValueError(
                f"ident() part must be a non-empty string, got: {part!r}"
            )
        if self.dialect == "snowflake":
            if "\x00" in part:
                raise ValueError(
                    "ident() part contains NUL, which is not permitted in "
                    "snowflake identifiers"
                )
            return '"' + part.replace('"', '""') + '"'
        # bigquery
        bad_chars = (
            ("\x00", "NUL"),
            ("\n", "newline"),
            ("\r", "carriage return"),
        )
        for bad, name in bad_chars:
            if bad in part:
                raise ValueError(
                    f"ident() part contains {name}, which is not permitted "
                    f"in bigquery identifiers"
                )
        escaped = part.replace("\\", "\\\\").replace("`", "\\`")
        return "`" + escaped + "`"
