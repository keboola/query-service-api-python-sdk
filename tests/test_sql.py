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


class TestLiteralStringSnowflake:
    def test_empty_string(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("", type="STRING").sql == "''"

    def test_simple_string(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("hello", type="STRING").sql == "'hello'"

    def test_escapes_single_quote(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("o'brien", type="STRING").sql == "'o''brien'"

    def test_escapes_backslash(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("a\\b", type="STRING").sql == "'a\\\\b'"

    def test_preserves_newline_literal(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("a\nb", type="STRING").sql == "'a\nb'"

    def test_rejects_nul(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="NUL"):
            sql.literal("a\x00b", type="STRING")

    def test_rejects_non_string_value(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError, match="STRING"):
            sql.literal(42, type="STRING")

    @pytest.mark.parametrize("alias", ["VARCHAR", "CHAR", "CHARACTER", "TEXT"])
    def test_string_aliases_emit_identically(self, alias: str) -> None:
        sql = SQL("snowflake")
        assert sql.literal("x", type=alias).sql == "'x'"


class TestLiteralStringBigQuery:
    def test_simple_string(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal("hello", type="STRING").sql == "'hello'"

    def test_escapes_single_quote(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal("o'brien", type="STRING").sql == "'o''brien'"


class TestLiteralNull:
    def test_none_emits_null_snowflake(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(None, type="STRING").sql == "NULL"

    def test_none_emits_null_bigquery(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(None, type="STRING").sql == "NULL"

    def test_unknown_type_still_raises_even_with_none(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="Unknown SQL type"):
            sql.literal(None, type="BANANA")


class TestLiteralCaseInsensitive:
    def test_lowercase_type_name(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("x", type="string").sql == "'x'"

    def test_mixed_case_type_name(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("x", type="String").sql == "'x'"


class TestLiteralUnknownType:
    def test_unknown_type_snowflake(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="Unknown SQL type 'BANANA'"):
            sql.literal("x", type="BANANA")

    def test_unknown_type_lists_supported(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="Supported: STRING"):
            sql.literal("x", type="BANANA")


class TestPackageExports:
    def test_sql_importable_from_package(self) -> None:
        from keboola_query_service import SQL as PackageSQL  # noqa: N811

        assert PackageSQL is SQL

    def test_safesql_importable_from_package(self) -> None:
        from keboola_query_service import SafeSql as PackageSafeSql

        assert PackageSafeSql is SafeSql

    def test_dialect_importable_from_package(self) -> None:
        from keboola_query_service import Dialect  # noqa: F401
