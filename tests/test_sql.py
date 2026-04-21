"""Tests for keboola_query_service.sql."""
from __future__ import annotations

import pytest

from keboola_query_service.sql import SQL, SafeSql


class TestDialect:
    def test_constructs_snowflake(self) -> None:
        sql = SQL("snowflake")
        assert sql.dialect == "snowflake"

    def test_constructs_bigquery(self) -> None:
        sql = SQL("bigquery")
        assert sql.dialect == "bigquery"

    def test_rejects_unknown_dialect(self) -> None:
        with pytest.raises(ValueError, match="Unknown dialect"):
            SQL("postgres")  # type: ignore[arg-type]


class TestSafeSql:
    def test_str_returns_sql(self) -> None:
        s = SafeSql(sql="RAW")
        assert str(s) == "RAW"


class TestRaw:
    def test_wraps_string_as_safesql(self) -> None:
        sql = SQL("snowflake")
        result = sql.raw("CURRENT_TIMESTAMP")
        assert isinstance(result, SafeSql)
        assert result.sql == "CURRENT_TIMESTAMP"

    def test_rejects_non_string(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError):
            sql.raw(123)  # type: ignore[arg-type]


class TestIdentSnowflake:
    def test_single_part(self) -> None:
        sql = SQL("snowflake")
        assert sql.ident("status").sql == '"status"'

    def test_multi_part_preserves_dots(self) -> None:
        sql = SQL("snowflake")
        assert sql.ident("in.c-main", "customers").sql == '"in.c-main"."customers"'

    def test_three_parts(self) -> None:
        sql = SQL("snowflake")
        assert (
            sql.ident("in.c-main", "customers", "id").sql
            == '"in.c-main"."customers"."id"'
        )

    def test_doubles_internal_double_quote(self) -> None:
        sql = SQL("snowflake")
        assert sql.ident('a"b').sql == '"a""b"'

    def test_allows_unicode_and_spaces(self) -> None:
        sql = SQL("snowflake")
        assert sql.ident("my table").sql == '"my table"'
        assert sql.ident("café").sql == '"café"'

    def test_rejects_zero_parts(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="at least one part"):
            sql.ident()

    def test_rejects_empty_string_part(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="non-empty string"):
            sql.ident("")

    def test_rejects_non_string_part(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError):
            sql.ident(42)  # type: ignore[arg-type]

    def test_rejects_nul(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="not permitted"):
            sql.ident("a\x00b")


class TestIdentBigQuery:
    def test_single_part_uses_backticks(self) -> None:
        sql = SQL("bigquery")
        assert sql.ident("status").sql == "`status`"

    def test_multi_part(self) -> None:
        sql = SQL("bigquery")
        assert sql.ident("project.dataset", "table").sql == "`project.dataset`.`table`"

    def test_escapes_backtick(self) -> None:
        sql = SQL("bigquery")
        assert sql.ident("a`b").sql == "`a\\`b`"

    def test_escapes_backslash(self) -> None:
        sql = SQL("bigquery")
        assert sql.ident("a\\b").sql == "`a\\\\b`"

    def test_rejects_newline(self) -> None:
        sql = SQL("bigquery")
        with pytest.raises(ValueError, match="newline"):
            sql.ident("a\nb")

    def test_rejects_carriage_return(self) -> None:
        sql = SQL("bigquery")
        with pytest.raises(ValueError, match="carriage return"):
            sql.ident("a\rb")

    def test_rejects_nul(self) -> None:
        sql = SQL("bigquery")
        with pytest.raises(ValueError, match="NUL"):
            sql.ident("a\x00b")
