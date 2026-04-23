"""Tests for keboola_query_service.sql."""
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal

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
        with pytest.raises(ValueError, match="Supported:.*STRING"):
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


class TestLiteralIntSnowflake:
    def test_positive(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(42, type="INT").sql == "42"

    def test_negative(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(-7, type="INT").sql == "-7"

    def test_zero(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(0, type="INT").sql == "0"

    def test_arbitrary_precision(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(10**100, type="INT").sql == "1" + "0" * 100

    def test_rejects_bool(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError, match="INT"):
            sql.literal(True, type="INT")

    def test_rejects_float(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError, match="INT"):
            sql.literal(1.5, type="INT")

    @pytest.mark.parametrize(
        "alias", ["INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"],
    )
    def test_int_aliases_emit_identically(self, alias: str) -> None:
        sql = SQL("snowflake")
        assert sql.literal(42, type=alias).sql == "42"


class TestLiteralInt64BigQuery:
    def test_positive(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(42, type="INT64").sql == "42"

    @pytest.mark.parametrize(
        "alias",
        ["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"],
    )
    def test_int64_aliases(self, alias: str) -> None:
        sql = SQL("bigquery")
        assert sql.literal(7, type=alias).sql == "7"

    def test_rejects_bool(self) -> None:
        sql = SQL("bigquery")
        with pytest.raises(TypeError, match="INT64"):
            sql.literal(True, type="INT64")


class TestLiteralNumberSnowflake:
    def test_int_input(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(42, type="NUMBER").sql == "42"

    def test_decimal_input(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(Decimal("3.14"), type="NUMBER").sql == "3.14"

    def test_string_input_valid(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("123.45", type="NUMBER").sql == "123.45"

    def test_string_negative(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("-0.5", type="NUMBER").sql == "-0.5"

    def test_string_invalid(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="NUMBER"):
            sql.literal("abc", type="NUMBER")

    def test_rejects_nan(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="non-finite"):
            sql.literal(Decimal("NaN"), type="NUMBER")

    def test_rejects_infinity(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="non-finite"):
            sql.literal(Decimal("Infinity"), type="NUMBER")

    def test_rejects_float(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError, match="NUMBER"):
            sql.literal(3.14, type="NUMBER")

    @pytest.mark.parametrize("alias", ["NUMERIC", "DECIMAL"])
    def test_aliases_snowflake(self, alias: str) -> None:
        sql = SQL("snowflake")
        assert sql.literal(1, type=alias).sql == "1"

    def test_scientific_notation_decimal_becomes_fixed(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(Decimal("1E+10"), type="NUMBER").sql == "10000000000"

    def test_small_scientific_notation_decimal(self) -> None:
        sql = SQL("snowflake")
        # Decimal("1E-2") should emit "0.01", not "1E-2"
        assert sql.literal(Decimal("1E-2"), type="NUMBER").sql == "0.01"


class TestLiteralNumericBigQuery:
    def test_int_input(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(42, type="NUMERIC").sql == "NUMERIC '42'"

    def test_decimal_input(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(Decimal("3.14"), type="NUMERIC").sql == "NUMERIC '3.14'"

    def test_decimal_alias(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(1, type="DECIMAL").sql == "NUMERIC '1'"

    def test_bignumeric(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(Decimal("9" * 40), type="BIGNUMERIC").sql == (
            "BIGNUMERIC '" + "9" * 40 + "'"
        )

    def test_bigdecimal_alias(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(1, type="BIGDECIMAL").sql == "BIGNUMERIC '1'"

    def test_scientific_notation_decimal_becomes_fixed(self) -> None:
        sql = SQL("bigquery")
        assert (
            sql.literal(Decimal("1E+10"), type="NUMERIC").sql
            == "NUMERIC '10000000000'"
        )


class TestLiteralFloatSnowflake:
    def test_float_value(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(1.5, type="FLOAT").sql == "1.5"

    def test_int_value(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(7, type="FLOAT").sql == "7"

    def test_precision_locked_in(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(0.1 + 0.2, type="FLOAT").sql == "0.30000000000000004"

    def test_scientific_notation(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(1e300, type="FLOAT").sql == "1e+300"

    def test_rejects_nan(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="non-finite"):
            sql.literal(float("nan"), type="FLOAT")

    def test_rejects_infinity(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="non-finite"):
            sql.literal(float("inf"), type="FLOAT")

    def test_rejects_bool(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError, match="FLOAT"):
            sql.literal(True, type="FLOAT")

    @pytest.mark.parametrize(
        "alias",
        ["FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"],
    )
    def test_aliases(self, alias: str) -> None:
        sql = SQL("snowflake")
        assert sql.literal(1.5, type=alias).sql == "1.5"


class TestLiteralFloat64BigQuery:
    def test_float_value(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(1.5, type="FLOAT64").sql == "1.5"

    def test_float_alias(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(1.5, type="FLOAT").sql == "1.5"


class TestLiteralBoolean:
    def test_true_snowflake(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(True, type="BOOLEAN").sql == "TRUE"

    def test_false_snowflake(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(False, type="BOOLEAN").sql == "FALSE"

    def test_true_bigquery(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(True, type="BOOL").sql == "TRUE"

    def test_bigquery_boolean_alias(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(False, type="BOOLEAN").sql == "FALSE"

    def test_rejects_int_1(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError, match="BOOLEAN"):
            sql.literal(1, type="BOOLEAN")

    def test_rejects_int_0(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError, match="BOOLEAN"):
            sql.literal(0, type="BOOLEAN")

    def test_rejects_string(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError, match="BOOLEAN"):
            sql.literal("true", type="BOOLEAN")


class TestLiteralDate:
    def test_date_snowflake(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal(date(2026, 4, 21), type="DATE").sql == "'2026-04-21'::DATE"

    def test_date_bigquery(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(date(2026, 4, 21), type="DATE").sql == "DATE '2026-04-21'"

    def test_string_snowflake(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("2026-04-21", type="DATE").sql == "'2026-04-21'::DATE"

    def test_malformed_string(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="DATE"):
            sql.literal("26-04-21", type="DATE")

    def test_rejects_datetime(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError, match="DATE"):
            sql.literal(datetime(2026, 4, 21, 12, 0), type="DATE")

    def test_rejects_int(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(TypeError, match="DATE"):
            sql.literal(20260421, type="DATE")


class TestLiteralTime:
    def test_time_snowflake(self) -> None:
        sql = SQL("snowflake")
        assert (
            sql.literal(time(12, 34, 56), type="TIME").sql == "'12:34:56'::TIME"
        )

    def test_time_microseconds(self) -> None:
        sql = SQL("snowflake")
        assert (
            sql.literal(time(12, 34, 56, 789000), type="TIME").sql
            == "'12:34:56.789000'::TIME"
        )

    def test_time_bigquery(self) -> None:
        sql = SQL("bigquery")
        assert sql.literal(time(12, 34, 56), type="TIME").sql == "TIME '12:34:56'"

    def test_string(self) -> None:
        sql = SQL("snowflake")
        assert sql.literal("12:34:56", type="TIME").sql == "'12:34:56'::TIME"

    def test_malformed_string(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="TIME"):
            sql.literal("not-a-time", type="TIME")
