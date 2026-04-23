"""SQL escape helper for the Keboola Query Service.

Dialect-bound factory producing safe SQL fragments. Callers declare the
SQL type of every literal explicitly via the ``type=`` keyword argument.
See docs/superpowers/specs/2026-04-21-sdk-quote-helper-design.md for the
design rationale.
"""
from __future__ import annotations

import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
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


def _emit_int_snowflake(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(
            f"literal(type='INT') expects int (not bool), got "
            f"{type(value).__name__}: {value!r}"
        )
    return str(value)


def _emit_int_bigquery(value: object) -> str:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(
            f"literal(type='INT64') expects int (not bool), got "
            f"{type(value).__name__}: {value!r}"
        )
    return str(value)


_NUMERIC_STRING_RE = re.compile(r"^-?\d+(\.\d+)?$")


def _coerce_decimal_string(value: object, type_label: str) -> str:
    if isinstance(value, bool):
        raise TypeError(
            f"literal(type={type_label!r}) expects int/Decimal/str (not bool), "
            f"got bool: {value!r}"
        )
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(
                f"Cannot escape non-finite Decimal: {value!r}. "
                f"NUMBER/NUMERIC literals do not support NaN/Infinity."
            )
        return f"{value:f}"
    if isinstance(value, str):
        if not _NUMERIC_STRING_RE.match(value):
            raise ValueError(
                f"literal(type={type_label!r}) string must match "
                f"[-]?\\d+(\\.\\d+)?, got: {value!r}"
            )
        return value
    raise TypeError(
        f"literal(type={type_label!r}) expects int, Decimal, or str, "
        f"got {type(value).__name__}: {value!r}"
    )


def _emit_number_snowflake(value: object) -> str:
    return _coerce_decimal_string(value, "NUMBER")


def _emit_numeric_bigquery(value: object) -> str:
    return f"NUMERIC '{_coerce_decimal_string(value, 'NUMERIC')}'"


def _emit_bignumeric_bigquery(value: object) -> str:
    return f"BIGNUMERIC '{_coerce_decimal_string(value, 'BIGNUMERIC')}'"


def _emit_float(value: object) -> str:
    if isinstance(value, bool):
        raise TypeError(
            f"literal(type='FLOAT') expects int/float (not bool), "
            f"got bool: {value!r}"
        )
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                f"Cannot escape non-finite float: {value!r}. "
                f"Snowflake and BigQuery literals do not support NaN/Infinity."
            )
        return repr(value)
    raise TypeError(
        f"literal(type='FLOAT') expects int or float, "
        f"got {type(value).__name__}: {value!r}"
    )


def _emit_boolean(value: object) -> str:
    if not isinstance(value, bool):
        raise TypeError(
            f"literal(type='BOOLEAN') expects bool, "
            f"got {type(value).__name__}: {value!r}"
        )
    return "TRUE" if value else "FALSE"


def _coerce_date(value: object, type_label: str) -> str:
    if isinstance(value, datetime):
        raise TypeError(
            f"literal(type={type_label!r}) expects datetime.date (not datetime). "
            f"If you have a datetime, pass value.date()."
        )
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value).isoformat()
        except ValueError as e:
            raise ValueError(
                f"literal(type={type_label!r}) expects 'YYYY-MM-DD', got: {value!r}"
            ) from e
    raise TypeError(
        f"literal(type={type_label!r}) expects datetime.date or 'YYYY-MM-DD', "
        f"got {type(value).__name__}: {value!r}"
    )


def _emit_date_snowflake(value: object) -> str:
    return f"'{_coerce_date(value, 'DATE')}'::DATE"


def _emit_date_bigquery(value: object) -> str:
    return f"DATE '{_coerce_date(value, 'DATE')}'"


_SNOWFLAKE_TYPES: dict[str, _DispatchEntry] = {
    "STRING": (("VARCHAR", "CHAR", "CHARACTER", "TEXT"), "str", _emit_string),
    "INT": (
        ("INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"),
        "int",
        _emit_int_snowflake,
    ),
    "NUMBER": (("NUMERIC", "DECIMAL"), "int|Decimal|str", _emit_number_snowflake),
    "FLOAT": (
        ("FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"),
        "int|float",
        _emit_float,
    ),
    "BOOLEAN": ((), "bool", _emit_boolean),
    "DATE": ((), "date|str", _emit_date_snowflake),
}

_BIGQUERY_TYPES: dict[str, _DispatchEntry] = {
    "STRING": ((), "str", _emit_string),
    "INT64": (
        ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"),
        "int",
        _emit_int_bigquery,
    ),
    "NUMERIC": (("DECIMAL",), "int|Decimal|str", _emit_numeric_bigquery),
    "BIGNUMERIC": (("BIGDECIMAL",), "int|Decimal|str", _emit_bignumeric_bigquery),
    "FLOAT64": (("FLOAT",), "int|float", _emit_float),
    "BOOL": (("BOOLEAN",), "bool", _emit_boolean),
    "DATE": ((), "date|str", _emit_date_bigquery),
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
