"""Tests for keboola_query_service.sql."""
from __future__ import annotations

import math

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


class TestLiteralPrimitives:
    def test_none_is_null(self) -> None:
        assert SQL("snowflake").literal(None).sql == "NULL"

    def test_true_is_TRUE(self) -> None:
        assert SQL("snowflake").literal(True).sql == "TRUE"

    def test_false_is_FALSE(self) -> None:
        assert SQL("snowflake").literal(False).sql == "FALSE"

    def test_int_zero(self) -> None:
        assert SQL("snowflake").literal(0).sql == "0"

    def test_int_negative(self) -> None:
        assert SQL("snowflake").literal(-1).sql == "-1"

    def test_int_large(self) -> None:
        assert SQL("snowflake").literal(10**100).sql == "1" + "0" * 100

    def test_float_simple(self) -> None:
        assert SQL("snowflake").literal(1.5).sql == "1.5"

    def test_float_round_trip_lockin(self) -> None:
        # Regression: do not round — faithful to the IEEE 754 double
        assert SQL("snowflake").literal(0.1 + 0.2).sql == "0.30000000000000004"

    def test_float_scientific_notation(self) -> None:
        # repr(1e300) is '1e+300' — accepted by both dialects
        assert SQL("snowflake").literal(1e300).sql == "1e+300"

    def test_reject_nan(self) -> None:
        with pytest.raises(ValueError, match="non-finite"):
            SQL("snowflake").literal(float("nan"))

    def test_reject_positive_infinity(self) -> None:
        with pytest.raises(ValueError, match="non-finite"):
            SQL("snowflake").literal(math.inf)

    def test_reject_negative_infinity(self) -> None:
        with pytest.raises(ValueError, match="non-finite"):
            SQL("snowflake").literal(-math.inf)


class TestLiteralBoolBeforeInt:
    """Regression: isinstance(True, int) is True — bool must dispatch first."""

    def test_true_is_not_1(self) -> None:
        assert SQL("snowflake").literal(True).sql == "TRUE"

    def test_false_is_not_0(self) -> None:
        assert SQL("snowflake").literal(False).sql == "FALSE"
