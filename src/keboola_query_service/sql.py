"""SQL escape helper for the Keboola Query Service.

Provides safe value interpolation into raw SQL strings for Snowflake and
BigQuery. See docs/superpowers/specs/2026-04-21-sdk-quote-helper-design.md
in connection-docs for the design rationale.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
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

    def literal(self, value: object) -> SafeSql:
        """Escape a Python value into a SQL literal fragment.

        Supported types: None, bool, int, float (finite), str, date,
        datetime, list/tuple, SafeSql. Unknown types raise TypeError
        with a message suggesting ``str(value)`` as a workaround for
        Decimal / UUID / bytes.

        Float note: values are emitted via ``repr()``. ``repr(0.1 + 0.2)``
        is ``'0.30000000000000004'`` — faithful to the IEEE 754 double.
        """
        # Order matters. SafeSql first so pre-escaped fragments pass through.
        if isinstance(value, SafeSql):
            return value
        if value is None:
            return SafeSql(sql="NULL")
        # bool must be checked before int — isinstance(True, int) is True.
        if isinstance(value, bool):
            return SafeSql(sql="TRUE" if value else "FALSE")
        if isinstance(value, int):
            return SafeSql(sql=str(value))
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError(
                    f"Cannot escape non-finite float: {value!r}. "
                    "Snowflake and BigQuery literals do not support NaN/Infinity."
                )
            return SafeSql(sql=repr(value))
        if isinstance(value, str):
            if "\x00" in value:
                raise ValueError(
                    "String literal contains NUL character, which neither "
                    "Snowflake nor BigQuery accept"
                )
            # Both dialects: escape backslash and single quote.
            escaped = value.replace("\\", "\\\\").replace("'", "''")
            return SafeSql(sql="'" + escaped + "'")
        # datetime.datetime must be checked before datetime.date —
        # datetime is a subclass of date.
        if isinstance(value, datetime):
            iso = value.isoformat(sep=" ")
            if self.dialect == "snowflake":
                kind = "TIMESTAMP_TZ" if value.tzinfo is not None else "TIMESTAMP_NTZ"
                return SafeSql(sql=f"'{iso}'::{kind}")
            # bigquery
            kind = "TIMESTAMP" if value.tzinfo is not None else "DATETIME"
            return SafeSql(sql=f"{kind} '{iso}'")
        if isinstance(value, date):
            iso = value.isoformat()
            if self.dialect == "snowflake":
                return SafeSql(sql=f"'{iso}'::DATE")
            return SafeSql(sql=f"DATE '{iso}'")
        if isinstance(value, (list, tuple)):
            if not value:
                return SafeSql(sql="(NULL)")
            parts: list[str] = []
            for elem in value:
                if isinstance(elem, (list, tuple)):
                    raise TypeError(
                        "Nested lists/tuples are not supported in SQL literals"
                    )
                parts.append(self.literal(elem).sql)
            return SafeSql(sql="(" + ", ".join(parts) + ")")
        raise TypeError(
            f"Cannot escape value of type {type(value).__name__}. "
            "Supported: None, bool, int, float, str, date, datetime, "
            "list/tuple, SafeSql. If you have a Decimal/UUID/bytes value, "
            "convert to str explicitly and pass that."
        )

    def date(self, value: date | str) -> SafeSql:
        """Emit a DATE literal explicitly.

        Accepts ``datetime.date`` or a ``"YYYY-MM-DD"`` string. Rejects
        ``datetime.datetime`` (callers should pass ``dt.date()``).
        """
        if isinstance(value, datetime):
            # Reject before the date branch below — datetime is-a date.
            raise TypeError(
                "date() expects datetime.date or a 'YYYY-MM-DD' string. "
                "If you have a datetime, pass value.date()."
            )
        if isinstance(value, date):
            d = value
        elif isinstance(value, str):
            try:
                d = date.fromisoformat(value)
            except ValueError as e:
                raise ValueError(
                    f"date() expects datetime.date or a 'YYYY-MM-DD' string, "
                    f"got: {value!r}"
                ) from e
        else:
            raise TypeError(
                f"date() expects datetime.date or a 'YYYY-MM-DD' string, "
                f"got: {value!r}"
            )
        iso = d.isoformat()
        if self.dialect == "snowflake":
            return SafeSql(sql=f"'{iso}'::DATE")
        return SafeSql(sql=f"DATE '{iso}'")

    def raw(self, s: str) -> SafeSql:
        """Wrap a string as a pre-escaped SafeSql fragment.

        Escape hatch for injecting SQL the helper doesn't directly support
        (e.g., ``CURRENT_TIMESTAMP``, backend-specific function calls).
        Use only with strings you fully control — never user input.
        """
        if not isinstance(s, str):
            raise TypeError(f"raw() requires str, got: {type(s).__name__}")
        return SafeSql(sql=s)

    def ident(self, *parts: str) -> SafeSql:
        """Quote one or more identifier parts and join with dots.

        Dots inside a part are preserved (never split). Each part is
        quoted and escaped per the active dialect:

        - Snowflake: wrap in ``"``, double internal ``"``, reject ``\\0``.
        - BigQuery:  wrap in `` ` ``, prefix internal `` ` `` and ``\\``
          with ``\\``, reject ``\\0``, ``\\n``, ``\\r``.
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
