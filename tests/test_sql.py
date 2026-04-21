"""Tests for keboola_query_service.sql."""
from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import UUID

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


class TestLiteralStrings:
    def test_empty_string(self) -> None:
        assert SQL("snowflake").literal("").sql == "''"

    def test_simple_string(self) -> None:
        assert SQL("snowflake").literal("hello").sql == "'hello'"

    def test_doubles_internal_single_quote(self) -> None:
        assert SQL("snowflake").literal("O'Brien").sql == "'O''Brien'"

    def test_snowflake_escapes_backslash(self) -> None:
        # Regression: Snowflake interprets \n as newline in string literals.
        # Python string "a\\nb" is 4 chars: a, \, n, b. We must emit
        # 'a\\nb' so Snowflake parses back to the same 4 chars.
        assert SQL("snowflake").literal("a\\nb").sql == "'a\\\\nb'"

    def test_bigquery_escapes_backslash(self) -> None:
        assert SQL("bigquery").literal("a\\nb").sql == "'a\\\\nb'"

    def test_literal_newline_preserved(self) -> None:
        # Actual newline (LF) byte, not the two-char \n escape —
        # passes through inside the quotes unchanged.
        assert SQL("snowflake").literal("a\nb").sql == "'a\nb'"

    def test_reject_nul_in_string(self) -> None:
        with pytest.raises(ValueError, match="NUL"):
            SQL("snowflake").literal("a\x00b")


class TestLiteralDates:
    def test_date_snowflake(self) -> None:
        assert SQL("snowflake").literal(date(2026, 4, 21)).sql == "'2026-04-21'::DATE"

    def test_date_bigquery(self) -> None:
        assert SQL("bigquery").literal(date(2026, 4, 21)).sql == "DATE '2026-04-21'"

    def test_naive_datetime_snowflake(self) -> None:
        dt = datetime(2026, 4, 21, 14, 30, 45, 123456)
        assert (
            SQL("snowflake").literal(dt).sql
            == "'2026-04-21 14:30:45.123456'::TIMESTAMP_NTZ"
        )

    def test_naive_datetime_bigquery(self) -> None:
        dt = datetime(2026, 4, 21, 14, 30, 45, 123456)
        assert (
            SQL("bigquery").literal(dt).sql
            == "DATETIME '2026-04-21 14:30:45.123456'"
        )

    def test_tz_aware_datetime_snowflake(self) -> None:
        tz = timezone(timedelta(hours=-7))
        dt = datetime(2026, 4, 21, 14, 30, 45, 123456, tzinfo=tz)
        assert (
            SQL("snowflake").literal(dt).sql
            == "'2026-04-21 14:30:45.123456-07:00'::TIMESTAMP_TZ"
        )

    def test_tz_aware_datetime_bigquery(self) -> None:
        tz = timezone(timedelta(hours=-7))
        dt = datetime(2026, 4, 21, 14, 30, 45, 123456, tzinfo=tz)
        assert (
            SQL("bigquery").literal(dt).sql
            == "TIMESTAMP '2026-04-21 14:30:45.123456-07:00'"
        )

    def test_datetime_utc(self) -> None:
        dt = datetime(2026, 4, 21, 14, 30, 0, tzinfo=timezone.utc)
        # Python formats +00:00 as +00:00 via isoformat
        assert "+00:00" in SQL("snowflake").literal(dt).sql

    def test_reject_time_alone(self) -> None:
        # time without a date is not supported — too ambiguous across backends
        with pytest.raises(TypeError):
            SQL("snowflake").literal(time(14, 30))


class TestLiteralLists:
    def test_non_empty_list(self) -> None:
        assert SQL("snowflake").literal([1, 2, 3]).sql == "(1, 2, 3)"

    def test_mixed_types(self) -> None:
        assert (
            SQL("snowflake").literal([1, "a", None, True]).sql
            == "(1, 'a', NULL, TRUE)"
        )

    def test_tuple_works_like_list(self) -> None:
        assert SQL("snowflake").literal((1, 2, 3)).sql == "(1, 2, 3)"

    def test_empty_list_is_NULL_tuple(self) -> None:
        # IN () is a syntax error; IN (NULL) returns no rows, matching empty-set.
        assert SQL("snowflake").literal([]).sql == "(NULL)"

    def test_empty_tuple_is_NULL_tuple(self) -> None:
        assert SQL("snowflake").literal(()).sql == "(NULL)"

    def test_nested_list_raises(self) -> None:
        with pytest.raises(TypeError, match="Nested"):
            SQL("snowflake").literal([1, [2, 3]])


class TestLiteralSafeSqlPassthrough:
    def test_literal_passes_safesql_unchanged(self) -> None:
        sql = SQL("snowflake")
        marker = sql.raw("CURRENT_TIMESTAMP")
        result = sql.literal(marker)
        assert result is marker  # identity-preserving

    def test_literal_passes_ident_unchanged(self) -> None:
        sql = SQL("snowflake")
        i = sql.ident("in.c-main", "customers")
        assert sql.literal(i) is i


class TestLiteralUnknownTypes:
    def test_rejects_decimal_with_str_hint(self) -> None:
        with pytest.raises(TypeError, match="convert to str"):
            SQL("snowflake").literal(Decimal("1.5"))

    def test_rejects_uuid(self) -> None:
        with pytest.raises(TypeError, match="convert to str"):
            SQL("snowflake").literal(UUID("00000000-0000-0000-0000-000000000000"))

    def test_rejects_bytes(self) -> None:
        with pytest.raises(TypeError, match="convert to str"):
            SQL("snowflake").literal(b"x")

    def test_rejects_custom_class(self) -> None:
        class Foo:
            pass

        with pytest.raises(TypeError):
            SQL("snowflake").literal(Foo())
