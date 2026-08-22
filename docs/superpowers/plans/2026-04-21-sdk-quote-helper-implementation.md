# SDK SQL Quote Helper Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a dialect-aware SQL quote/escape helper to the Keboola Query Service Python and JS SDKs so users can interpolate untrusted values into SQL safely.

**Architecture:** Two mirror implementations — Python (`src/keboola_query_service/sql.py`) and JS (`src/sql.ts`) — producing byte-identical output for the same inputs. Python is implemented first as the reference; JS is then ported with the Python test fixtures used as a cross-language correctness contract.

**Tech Stack:**
- Python SDK: `hatchling` build, `httpx` runtime dep, `pytest` + `pytest-asyncio` + `ruff` + `mypy` (strict) for dev, Python 3.10+.
- JS SDK: `tsup` build, no runtime deps, `vitest` + `eslint` + `tsc` for dev, Node 18+.
- Spec: `/Users/miroslavcillik/Projects/connection-docs/docs/superpowers/specs/2026-04-21-sdk-quote-helper-design.md` (branch `sdk-quote-helper-spec`). Read it before starting — task code examples assume familiarity with its escape rules and decisions.

**Scope:**
- Phase A — Python SDK (reference implementation): tasks 1–12.
- Phase B — JS SDK (port): tasks 13–22.
- Phase C — cross-language byte-equality check + release-prep checklist: tasks 23–24 (non-gating).

**Out of scope for this plan** (spec followups):
- Updating `connection-docs/data-apps/storage-access/index.md` to use the helper.
- Fixing the `keboola.query-service-client` vs `keboola-query-service` naming mismatch in the docs.
- Publishing releases to PyPI / npm (task 24 prepares the release but does not execute it — releases happen after human sign-off).

---

## Repo layout (reference)

**Python SDK** (`/Users/miroslavcillik/Projects/query-service-api-python-sdk`):
```
src/keboola_query_service/
  __init__.py        # exports — will be modified
  _version.py
  client.py
  exceptions.py
  models.py
  py.typed
  sql.py             # NEW — this plan creates it
tests/
  __init__.py
  test_client.py
  test_sql.py        # NEW — this plan creates it
pyproject.toml
README.md
```

**JS SDK** (`/Users/miroslavcillik/Projects/query-service-api-js-sdk`):
```
src/
  client.ts
  errors.ts
  index.ts           # exports — will be modified
  types.ts
  sql.ts             # NEW — this plan creates it
tests/
  client.test.ts
  sql.test.ts        # NEW — this plan creates it
package.json
README.md
```

## Cross-repo branching

Each SDK uses its own branch with Keboola git conventions (see `git-conventions.md`):
- Python SDK: `feat/sdk-quote-helper`
- JS SDK: `feat/sdk-quote-helper`

Commit style: Conventional Commits with `()` scope. Examples: `feat(sql): add SQL.literal`, `test(sql): cover backslash escape`.

---

## Phase A — Python SDK

### Task 1: Scaffold `sql.py` module with types

**Files:**
- Create: `src/keboola_query_service/sql.py`
- Test: `tests/test_sql.py`

- [ ] **Step 1: Create the Python SDK branch**

```bash
cd /Users/miroslavcillik/Projects/query-service-api-python-sdk
git checkout -b feat/sdk-quote-helper main
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_sql.py`:

```python
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
```

- [ ] **Step 3: Run test to verify it fails**

Run:
```bash
docker compose run --rm dev pytest tests/test_sql.py -v || \
pytest tests/test_sql.py -v
```

Expected: `ModuleNotFoundError: No module named 'keboola_query_service.sql'`.

(Use whichever command the repo's existing dev workflow supports — check `README.md` for the canonical invocation.)

- [ ] **Step 4: Write minimal implementation**

Create `src/keboola_query_service/sql.py`:

```python
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_sql.py -v`
Expected: 3 passed.

- [ ] **Step 6: Run ruff + mypy (strict) before committing**

```bash
ruff check src/keboola_query_service/sql.py tests/test_sql.py
mypy src/keboola_query_service/sql.py
```

Expected: no errors. If any, fix before committing (mypy is configured `strict` in `pyproject.toml`).

- [ ] **Step 7: Commit**

```bash
git add src/keboola_query_service/sql.py tests/test_sql.py
git commit -m "feat(sql): scaffold SQL helper module with Dialect + SafeSql"
```

---

### Task 2: `SQL.raw()`

**Files:**
- Modify: `src/keboola_query_service/sql.py`
- Modify: `tests/test_sql.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sql.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sql.py::TestRaw -v`
Expected: `AttributeError: 'SQL' object has no attribute 'raw'`.

- [ ] **Step 3: Implement `raw`**

Add method to `SQL` in `src/keboola_query_service/sql.py`:

```python
    def raw(self, s: str) -> SafeSql:
        """Wrap a string as a pre-escaped SafeSql fragment.

        Escape hatch for injecting SQL the helper doesn't directly support
        (e.g., ``CURRENT_TIMESTAMP``, backend-specific function calls).
        Use only with strings you fully control — never user input.
        """
        if not isinstance(s, str):
            raise TypeError(f"raw() requires str, got: {type(s).__name__}")
        return SafeSql(sql=s)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sql.py::TestRaw -v`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add src/keboola_query_service/sql.py tests/test_sql.py
git commit -m "feat(sql): add SQL.raw() escape hatch"
```

---

### Task 3: `SQL.ident()` — Snowflake

**Files:**
- Modify: `src/keboola_query_service/sql.py`
- Modify: `tests/test_sql.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sql.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sql.py::TestIdentSnowflake -v`
Expected: all fail with `AttributeError: 'SQL' object has no attribute 'ident'`.

- [ ] **Step 3: Implement `ident`**

Add to `SQL`:

```python
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
        for bad, name in (("\x00", "NUL"), ("\n", "newline"), ("\r", "carriage return")):
            if bad in part:
                raise ValueError(
                    f"ident() part contains {name}, which is not permitted "
                    f"in bigquery identifiers"
                )
        escaped = part.replace("\\", "\\\\").replace("`", "\\`")
        return "`" + escaped + "`"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sql.py::TestIdentSnowflake -v`
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add src/keboola_query_service/sql.py tests/test_sql.py
git commit -m "feat(sql): add SQL.ident() with Snowflake quoting"
```

---

### Task 4: `SQL.ident()` — BigQuery

**Files:**
- Modify: `tests/test_sql.py` (implementation already handles BigQuery from Task 3)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sql.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `pytest tests/test_sql.py::TestIdentBigQuery -v`
Expected: 7 passed. (Implementation from Task 3 already covers BigQuery.)

- [ ] **Step 3: Commit**

```bash
git add tests/test_sql.py
git commit -m "test(sql): cover BigQuery identifier quoting"
```

---

### Task 5: `SQL.literal()` — primitives (None, bool, int, float)

**Files:**
- Modify: `src/keboola_query_service/sql.py`
- Modify: `tests/test_sql.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sql.py`:

```python
import math


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
        # Arbitrary-precision
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sql.py::TestLiteralPrimitives tests/test_sql.py::TestLiteralBoolBeforeInt -v`
Expected: all fail with `AttributeError: 'SQL' object has no attribute 'literal'`.

- [ ] **Step 3: Implement `literal` primitive dispatch**

Add to `SQL`, and add the `math` import at the top of `sql.py`:

```python
# Top of sql.py (after existing imports):
import math
```

```python
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
        raise TypeError(
            f"Cannot escape value of type {type(value).__name__}. "
            "Supported: None, bool, int, float, str, date, datetime, "
            "list/tuple, SafeSql. If you have a Decimal/UUID/bytes value, "
            "convert to str explicitly and pass that."
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sql.py::TestLiteralPrimitives tests/test_sql.py::TestLiteralBoolBeforeInt -v`
Expected: 14 passed.

- [ ] **Step 5: Commit**

```bash
git add src/keboola_query_service/sql.py tests/test_sql.py
git commit -m "feat(sql): add SQL.literal() for None, bool, int, float"
```

---

### Task 6: `SQL.literal()` — strings (with Snowflake backslash regression)

**Files:**
- Modify: `src/keboola_query_service/sql.py`
- Modify: `tests/test_sql.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sql.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sql.py::TestLiteralStrings -v`
Expected: all fail — `literal` currently has no `str` branch.

- [ ] **Step 3: Add string branch to `literal`**

Insert the following in `SQL.literal` — **before** the `raise TypeError` at the end:

```python
        if isinstance(value, str):
            if "\x00" in value:
                raise ValueError(
                    "String literal contains NUL character, which neither "
                    "Snowflake nor BigQuery accept"
                )
            # Both dialects: escape backslash and single quote.
            escaped = value.replace("\\", "\\\\").replace("'", "''")
            return SafeSql(sql="'" + escaped + "'")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sql.py::TestLiteralStrings -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/keboola_query_service/sql.py tests/test_sql.py
git commit -m "feat(sql): escape strings including backslash (Snowflake correctness)"
```

---

### Task 7: `SQL.literal()` — dates and datetimes

**Files:**
- Modify: `src/keboola_query_service/sql.py`
- Modify: `tests/test_sql.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sql.py`:

```python
from datetime import date, datetime, time, timedelta, timezone


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sql.py::TestLiteralDates -v`
Expected: all fail — `literal` has no `date`/`datetime` branch.

- [ ] **Step 3: Add date/datetime branches**

Insert in `SQL.literal` **after** the `str` branch and **before** the `raise TypeError`:

```python
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
```

Add `from datetime import date, datetime` to the top of `sql.py` (alongside existing imports).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sql.py::TestLiteralDates -v`
Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add src/keboola_query_service/sql.py tests/test_sql.py
git commit -m "feat(sql): escape date and datetime literals per dialect"
```

---

### Task 8: `SQL.literal()` — lists and tuples

**Files:**
- Modify: `src/keboola_query_service/sql.py`
- Modify: `tests/test_sql.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sql.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sql.py::TestLiteralLists -v`
Expected: all fail — `literal` has no list branch.

- [ ] **Step 3: Add list/tuple branch**

Insert in `SQL.literal` **after** the `str` branch (but before `datetime` dispatch is fine, or place it just before `raise TypeError` — the nested-check rejects re-entry):

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sql.py::TestLiteralLists -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/keboola_query_service/sql.py tests/test_sql.py
git commit -m "feat(sql): escape list/tuple literals with IN-clause-safe empty behavior"
```

---

### Task 9: `SQL.literal()` — SafeSql passthrough and unknown types

**Files:**
- Modify: `tests/test_sql.py` (implementation already handles from Task 5)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sql.py`:

```python
from decimal import Decimal
from uuid import UUID


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
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `pytest tests/test_sql.py::TestLiteralSafeSqlPassthrough tests/test_sql.py::TestLiteralUnknownTypes -v`
Expected: 6 passed. (SafeSql passthrough and unknown-type error were implemented in Task 5.)

- [ ] **Step 3: Commit**

```bash
git add tests/test_sql.py
git commit -m "test(sql): cover SafeSql passthrough and unknown-type errors"
```

---

### Task 10: `SQL.date()`

**Files:**
- Modify: `src/keboola_query_service/sql.py`
- Modify: `tests/test_sql.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sql.py`:

```python
class TestDate:
    def test_date_from_date_object(self) -> None:
        assert (
            SQL("snowflake").date(date(2026, 4, 21)).sql == "'2026-04-21'::DATE"
        )

    def test_date_from_string(self) -> None:
        assert SQL("snowflake").date("2026-04-21").sql == "'2026-04-21'::DATE"

    def test_date_bigquery(self) -> None:
        assert SQL("bigquery").date("2026-04-21").sql == "DATE '2026-04-21'"

    def test_rejects_malformed_string(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError):
            sql.date("2026/04/21")
        with pytest.raises(ValueError):
            sql.date("not-a-date")

    def test_rejects_datetime_requires_explicit_date(self) -> None:
        # Callers with a datetime should pass .date() themselves
        sql = SQL("snowflake")
        with pytest.raises(TypeError):
            sql.date(datetime(2026, 4, 21, 12, 0))  # type: ignore[arg-type]

    def test_rejects_non_date_non_string(self) -> None:
        with pytest.raises(TypeError):
            SQL("snowflake").date(42)  # type: ignore[arg-type]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sql.py::TestDate -v`
Expected: all fail — `SQL.date` does not exist.

- [ ] **Step 3: Implement `date`**

Add to `SQL`:

```python
    def date(self, value: "date | str") -> SafeSql:
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sql.py::TestDate -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/keboola_query_service/sql.py tests/test_sql.py
git commit -m "feat(sql): add SQL.date() for explicit DATE literals"
```

---

### Task 11: `SQL.format()` with `_SafeFormatter`

**Files:**
- Modify: `src/keboola_query_service/sql.py`
- Modify: `tests/test_sql.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_sql.py`:

```python
class TestFormat:
    def test_single_named_placeholder_escapes_value(self) -> None:
        sql = SQL("snowflake")
        assert sql.format("SET x = {v}", v="O'Brien") == "SET x = 'O''Brien'"

    def test_multiple_named_placeholders(self) -> None:
        sql = SQL("snowflake")
        result = sql.format(
            "SET a = {a}, b = {b}, c = {c}",
            a=1, b="two", c=None,
        )
        assert result == "SET a = 1, b = 'two', c = NULL"

    def test_safesql_value_passes_through(self) -> None:
        sql = SQL("snowflake")
        result = sql.format(
            "UPDATE {t} SET x = {x}",
            t=sql.ident("in.c-main", "approvals"),
            x="o'brien",
        )
        assert (
            result
            == "UPDATE \"in.c-main\".\"approvals\" SET x = 'o''brien'"
        )

    def test_attribute_reference_still_escapes(self) -> None:
        sql = SQL("snowflake")
        class Obj:
            name = "o'brien"
        assert sql.format("x = {obj.name}", obj=Obj()) == "x = 'o''brien'"

    def test_index_reference_still_escapes(self) -> None:
        sql = SQL("snowflake")
        assert sql.format("x = {xs[0]}", xs=["o'brien"]) == "x = 'o''brien'"

    def test_positional_placeholder_raises(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(IndexError):
            sql.format("x = {0}", "one")  # type: ignore[misc]

    def test_format_spec_raises(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(ValueError, match="Format spec"):
            sql.format("price = {p:.2f}", p=1.2345)

    def test_unknown_named_placeholder_raises(self) -> None:
        sql = SQL("snowflake")
        with pytest.raises(KeyError):
            sql.format("SET x = {y}", a=1)

    def test_literal_braces(self) -> None:
        sql = SQL("snowflake")
        assert sql.format("WHERE j = '{{\"k\": {v}}}'", v=1) == "WHERE j = '{\"k\": 1}'"

    def test_end_to_end_docs_example(self) -> None:
        sql = SQL("snowflake")
        q = sql.format(
            "UPDATE {t} SET status = {status}, updated_at = {ts} WHERE id = {id}",
            t=sql.ident("in.c-main", "approvals"),
            status="approved",
            ts=sql.raw("CURRENT_TIMESTAMP"),
            id=123,
        )
        assert (
            q
            == "UPDATE \"in.c-main\".\"approvals\" "
            "SET status = 'approved', updated_at = CURRENT_TIMESTAMP "
            "WHERE id = 123"
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sql.py::TestFormat -v`
Expected: all fail — `SQL.format` does not exist.

- [ ] **Step 3: Implement `_SafeFormatter` and `format`**

Add `import string` to the top of `sql.py`. Add the formatter class at module scope (after `SafeSql`, before `SQL`):

```python
import string


class _SafeFormatter(string.Formatter):
    """Formatter that routes each interpolated value through SQL.literal().

    Rejects non-empty format_spec (e.g., ``{p:.2f}``) with ValueError.
    SafeSql values are passed through unchanged.
    """

    def __init__(self, sql: "SQL") -> None:
        self._sql = sql

    def format_field(self, value: object, format_spec: str) -> str:
        if format_spec:
            raise ValueError(
                f"Format spec {format_spec!r} is not supported in sql.format(). "
                "Pre-format the value (e.g., round() or strftime()) before passing."
            )
        if isinstance(value, SafeSql):
            return value.sql
        return self._sql.literal(value).sql
```

Add `format` to `SQL`:

```python
    def format(self, template: str, **values: object) -> str:
        """Interpolate ``{name}`` placeholders in ``template`` safely.

        Each resolved value is routed through ``SafeSql`` passthrough +
        ``literal()``. Only named placeholders are supported in v1;
        ``{0}`` / ``{}`` raise ``IndexError``. Non-empty format specs
        (``{p:.2f}``) raise ``ValueError``. Standard brace escapes
        (``{{``, ``}}``) apply.
        """
        return _SafeFormatter(self).vformat(template, (), values)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sql.py::TestFormat -v`
Expected: 10 passed.

- [ ] **Step 5: Run the full suite + lint + mypy**

```bash
pytest tests/ -v
ruff check src/ tests/
mypy src/keboola_query_service/sql.py
```

Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add src/keboola_query_service/sql.py tests/test_sql.py
git commit -m "feat(sql): add SQL.format() with SafeSql passthrough"
```

---

### Task 12: Export from `__init__.py`

**Files:**
- Modify: `src/keboola_query_service/__init__.py`
- Modify: `tests/test_sql.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_sql.py`:

```python
class TestPackageExports:
    def test_sql_importable_from_package(self) -> None:
        from keboola_query_service import SQL as PackageSQL  # noqa: N811

        assert PackageSQL is SQL

    def test_safesql_importable_from_package(self) -> None:
        from keboola_query_service import SafeSql as PackageSafeSql

        assert PackageSafeSql is SafeSql

    def test_dialect_importable_from_package(self) -> None:
        from keboola_query_service import Dialect  # noqa: F401
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_sql.py::TestPackageExports -v`
Expected: `ImportError: cannot import name 'SQL'` (and similar for SafeSql / Dialect).

- [ ] **Step 3: Add exports**

Modify `src/keboola_query_service/__init__.py`. Add to imports block (after the existing `from .models import ...`):

```python
from .sql import SQL, Dialect, SafeSql
```

Add `"SQL"`, `"SafeSql"`, `"Dialect"` to the `__all__` list:

```python
__all__ = [
    "__version__",
    "Client",
    "ActorType",
    "JobState",
    "StatementState",
    "Column",
    "Statement",
    "JobStatus",
    "QueryResult",
    "QueryHistory",
    "QueryServiceError",
    "AuthenticationError",
    "ValidationError",
    "NotFoundError",
    "JobError",
    "JobTimeoutError",
    "SQL",
    "SafeSql",
    "Dialect",
]
```

- [ ] **Step 4: Run full suite + type-check + lint**

```bash
pytest tests/ -v
ruff check src/ tests/
mypy src/keboola_query_service/
```

Expected: all green; 0 mypy errors (strict mode).

- [ ] **Step 5: Commit + push**

```bash
git add src/keboola_query_service/__init__.py tests/test_sql.py
git commit -m "feat(sql): export SQL, SafeSql, Dialect from package root"
git push -u origin feat/sdk-quote-helper
```

**Phase A checkpoint.** Open a draft PR against `main` in the Python SDK repo. Do not merge yet — Phase C cross-verification uses this branch.

---

## Phase B — JS SDK

### Task 13: Scaffold `sql.ts` module with types

**Files:**
- Create: `/Users/miroslavcillik/Projects/query-service-api-js-sdk/src/sql.ts`
- Create: `/Users/miroslavcillik/Projects/query-service-api-js-sdk/tests/sql.test.ts`

- [ ] **Step 1: Create the JS SDK branch**

```bash
cd /Users/miroslavcillik/Projects/query-service-api-js-sdk
git checkout -b feat/sdk-quote-helper main
```

- [ ] **Step 2: Write the failing test**

Create `tests/sql.test.ts`:

```typescript
import { describe, expect, it } from "vitest";

import { createSql, type SafeSql } from "../src/sql";

describe("createSql", () => {
  it("constructs snowflake", () => {
    const sql = createSql("snowflake");
    expect(sql).toBeTypeOf("function");
  });

  it("constructs bigquery", () => {
    const sql = createSql("bigquery");
    expect(sql).toBeTypeOf("function");
  });

  it("rejects unknown dialect", () => {
    // @ts-expect-error invalid dialect
    expect(() => createSql("postgres")).toThrow(TypeError);
  });
});

describe("SafeSql", () => {
  it("is a branded object with .sql", () => {
    const sql = createSql("snowflake");
    const s: SafeSql = sql.raw("X");
    expect(s.__safe).toBe(true);
    expect(s.sql).toBe("X");
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

```bash
npm test -- sql.test
```

Expected: fails — `src/sql.ts` not found (or import resolves to nothing).

- [ ] **Step 4: Write minimal implementation**

Create `src/sql.ts`:

```typescript
/**
 * SQL escape helper for the Keboola Query Service.
 *
 * Mirrors keboola_query_service.sql in the Python SDK. See
 * docs/superpowers/specs/2026-04-21-sdk-quote-helper-design.md in
 * connection-docs for the design rationale.
 */

export type Dialect = "snowflake" | "bigquery";

const VALID_DIALECTS: readonly Dialect[] = ["snowflake", "bigquery"];

export interface SafeSql {
  readonly __safe: true;
  readonly sql: string;
}

/** Returned by {@link createSql}. Callable tag function with helper methods. */
export interface Sql {
  (strings: TemplateStringsArray, ...values: unknown[]): string;
  literal(value: unknown): SafeSql;
  ident(...parts: string[]): SafeSql;
  date(value: Date | string): SafeSql;
  raw(s: string): SafeSql;
  readonly dialect: Dialect;
}

function makeSafe(sql: string): SafeSql {
  return { __safe: true, sql };
}

function isSafeSql(v: unknown): v is SafeSql {
  return (
    typeof v === "object" &&
    v !== null &&
    (v as { __safe?: unknown }).__safe === true
  );
}

export function createSql(dialect: Dialect): Sql {
  if (!VALID_DIALECTS.includes(dialect)) {
    throw new TypeError(
      `Unknown dialect: ${JSON.stringify(dialect)}. ` +
        `Supported: 'snowflake', 'bigquery'`,
    );
  }

  function raw(s: string): SafeSql {
    if (typeof s !== "string") {
      throw new TypeError(`raw() requires string, got: ${typeof s}`);
    }
    return makeSafe(s);
  }

  // Tag function — populated in subsequent tasks.
  function tag(_strings: TemplateStringsArray, ..._values: unknown[]): string {
    throw new Error("sql`...` not implemented yet");
  }

  const api = tag as Sql;
  // Attach methods.
  Object.defineProperty(api, "dialect", { value: dialect, enumerable: true });
  api.raw = raw;
  // literal, ident, date added in later tasks.
  api.literal = () => {
    throw new Error("literal() not implemented yet");
  };
  api.ident = () => {
    throw new Error("ident() not implemented yet");
  };
  api.date = () => {
    throw new Error("date() not implemented yet");
  };
  return api;
}
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
npm test -- sql.test
```

Expected: 4 passed.

- [ ] **Step 6: Run lint + typecheck**

```bash
npm run lint
npm run typecheck
```

Expected: 0 errors.

- [ ] **Step 7: Commit**

```bash
git add src/sql.ts tests/sql.test.ts
git commit -m "feat(sql): scaffold sql.ts with createSql + SafeSql branded type"
```

---

### Task 14: `sql.ident()` — both dialects

**Files:**
- Modify: `src/sql.ts`
- Modify: `tests/sql.test.ts`

- [ ] **Step 1: Write the failing tests**

Append to `tests/sql.test.ts`:

```typescript
describe("ident - snowflake", () => {
  const sql = createSql("snowflake");

  it("quotes single part", () => {
    expect(sql.ident("status").sql).toBe('"status"');
  });

  it("preserves dots in multi-part", () => {
    expect(sql.ident("in.c-main", "customers").sql).toBe(
      '"in.c-main"."customers"',
    );
  });

  it("doubles internal double quote", () => {
    expect(sql.ident('a"b').sql).toBe('"a""b"');
  });

  it("allows unicode and spaces", () => {
    expect(sql.ident("my table").sql).toBe('"my table"');
    expect(sql.ident("café").sql).toBe('"café"');
  });

  it("rejects zero parts", () => {
    expect(() => sql.ident()).toThrow(TypeError);
  });

  it("rejects empty string", () => {
    expect(() => sql.ident("")).toThrow(TypeError);
  });

  it("rejects NUL", () => {
    expect(() => sql.ident("a\x00b")).toThrow(TypeError);
  });
});

describe("ident - bigquery", () => {
  const sql = createSql("bigquery");

  it("uses backticks", () => {
    expect(sql.ident("status").sql).toBe("`status`");
  });

  it("multi-part", () => {
    expect(sql.ident("project.dataset", "table").sql).toBe(
      "`project.dataset`.`table`",
    );
  });

  it("escapes backtick", () => {
    expect(sql.ident("a`b").sql).toBe("`a\\`b`");
  });

  it("escapes backslash", () => {
    expect(sql.ident("a\\b").sql).toBe("`a\\\\b`");
  });

  it("rejects newline", () => {
    expect(() => sql.ident("a\nb")).toThrow(TypeError);
  });

  it("rejects carriage return", () => {
    expect(() => sql.ident("a\rb")).toThrow(TypeError);
  });

  it("rejects NUL", () => {
    expect(() => sql.ident("a\x00b")).toThrow(TypeError);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
npm test -- sql.test
```

Expected: fails — `ident() not implemented yet`.

- [ ] **Step 3: Implement `ident`**

Replace the `ident` stub inside `createSql` in `src/sql.ts` with:

```typescript
  function ident(...parts: string[]): SafeSql {
    if (parts.length === 0) {
      throw new TypeError("ident() requires at least one part");
    }
    const escaped = parts.map((p) => quoteIdentPart(p));
    return makeSafe(escaped.join("."));
  }

  function quoteIdentPart(part: unknown): string {
    if (typeof part !== "string") {
      throw new TypeError(
        `ident() part must be a non-empty string, got: ${String(part)}`,
      );
    }
    if (part === "") {
      throw new TypeError(
        `ident() part must be a non-empty string, got: ${JSON.stringify(part)}`,
      );
    }
    if (dialect === "snowflake") {
      if (part.includes("\x00")) {
        throw new TypeError(
          "ident() part contains NUL, which is not permitted in snowflake identifiers",
        );
      }
      return `"${part.replace(/"/g, '""')}"`;
    }
    // bigquery
    const rejects: [string, string][] = [
      ["\x00", "NUL"],
      ["\n", "newline"],
      ["\r", "carriage return"],
    ];
    for (const [bad, name] of rejects) {
      if (part.includes(bad)) {
        throw new TypeError(
          `ident() part contains ${name}, which is not permitted in bigquery identifiers`,
        );
      }
    }
    const escaped = part.replace(/\\/g, "\\\\").replace(/`/g, "\\`");
    return `\`${escaped}\``;
  }
```

Then, in the `api` assembly block, replace:

```typescript
  api.ident = () => {
    throw new Error("ident() not implemented yet");
  };
```

with:

```typescript
  api.ident = ident;
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
npm test -- sql.test
```

Expected: all ident tests pass (14 in this task).

- [ ] **Step 5: Commit**

```bash
git add src/sql.ts tests/sql.test.ts
git commit -m "feat(sql): add ident() for Snowflake and BigQuery"
```

---

### Task 15: `sql.literal()` — primitives (null, boolean, number, bigint, string)

**Files:**
- Modify: `src/sql.ts`
- Modify: `tests/sql.test.ts`

- [ ] **Step 1: Write the failing tests**

Append to `tests/sql.test.ts`:

```typescript
describe("literal - primitives", () => {
  const sql = createSql("snowflake");

  it("null → NULL", () => expect(sql.literal(null).sql).toBe("NULL"));
  it("undefined → NULL", () => expect(sql.literal(undefined).sql).toBe("NULL"));

  it("true → TRUE", () => expect(sql.literal(true).sql).toBe("TRUE"));
  it("false → FALSE", () => expect(sql.literal(false).sql).toBe("FALSE"));

  it("number int", () => expect(sql.literal(42).sql).toBe("42"));
  it("number negative", () => expect(sql.literal(-1).sql).toBe("-1"));
  it("number float", () => expect(sql.literal(1.5).sql).toBe("1.5"));
  it("number 0.1+0.2 round-trip lockin", () => {
    expect(sql.literal(0.1 + 0.2).sql).toBe("0.30000000000000004");
  });
  it("number 1e300 scientific notation", () => {
    expect(sql.literal(1e300).sql).toBe("1e+300");
  });

  it("bigint → decimal", () => expect(sql.literal(42n).sql).toBe("42"));
  it("bigint large", () => {
    expect(sql.literal(10n ** 100n).sql).toBe("1" + "0".repeat(100));
  });

  it("rejects NaN", () => {
    expect(() => sql.literal(NaN)).toThrow(RangeError);
  });
  it("rejects Infinity", () => {
    expect(() => sql.literal(Infinity)).toThrow(RangeError);
  });
  it("rejects -Infinity", () => {
    expect(() => sql.literal(-Infinity)).toThrow(RangeError);
  });
});

describe("literal - strings", () => {
  it("empty string", () => {
    expect(createSql("snowflake").literal("").sql).toBe("''");
  });
  it("doubles internal single quote", () => {
    expect(createSql("snowflake").literal("O'Brien").sql).toBe("'O''Brien'");
  });
  it("escapes backslash (snowflake regression)", () => {
    // Source string is 4 chars: a, \, n, b
    expect(createSql("snowflake").literal("a\\nb").sql).toBe("'a\\\\nb'");
  });
  it("escapes backslash (bigquery)", () => {
    expect(createSql("bigquery").literal("a\\nb").sql).toBe("'a\\\\nb'");
  });
  it("preserves literal newline byte", () => {
    expect(createSql("snowflake").literal("a\nb").sql).toBe("'a\nb'");
  });
  it("rejects NUL", () => {
    expect(() => createSql("snowflake").literal("a\x00b")).toThrow(TypeError);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
npm test -- sql.test
```

Expected: fails — `literal() not implemented yet`.

- [ ] **Step 3: Implement `literal` primitives**

Replace the `literal` stub inside `createSql` in `src/sql.ts` with:

```typescript
  function literal(value: unknown): SafeSql {
    if (isSafeSql(value)) {
      return value;
    }
    if (value === null || value === undefined) {
      return makeSafe("NULL");
    }
    if (typeof value === "boolean") {
      return makeSafe(value ? "TRUE" : "FALSE");
    }
    if (typeof value === "bigint") {
      return makeSafe(String(value));
    }
    if (typeof value === "number") {
      if (!Number.isFinite(value)) {
        throw new RangeError(
          `Cannot escape non-finite number: ${value}. ` +
            `Snowflake and BigQuery literals do not support NaN/Infinity.`,
        );
      }
      return makeSafe(String(value));
    }
    if (typeof value === "string") {
      if (value.includes("\x00")) {
        throw new TypeError(
          "String literal contains NUL character, which neither Snowflake nor BigQuery accept",
        );
      }
      const escaped = value.replace(/\\/g, "\\\\").replace(/'/g, "''");
      return makeSafe(`'${escaped}'`);
    }
    // Further types (Date, Array) added in later tasks.
    throw new TypeError(
      `Cannot escape value of type ${typeof value}. ` +
        `Supported: null/undefined, boolean, number, bigint, string, Date, ` +
        `Array, SafeSql. If you have a Decimal/BigDecimal/UUID/Buffer value, ` +
        `convert to string explicitly and pass that.`,
    );
  }
```

Replace `api.literal = ...` stub with `api.literal = literal;`.

- [ ] **Step 4: Run tests to verify they pass**

```bash
npm test -- sql.test
```

Expected: 20+ tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql.ts tests/sql.test.ts
git commit -m "feat(sql): literal() for null, boolean, number, bigint, string"
```

---

### Task 16: `sql.literal()` — `Date` (always instant, UTC)

**Files:**
- Modify: `src/sql.ts`
- Modify: `tests/sql.test.ts`

- [ ] **Step 1: Write the failing tests**

Append to `tests/sql.test.ts`:

```typescript
describe("literal - Date", () => {
  it("snowflake emits TIMESTAMP_TZ in UTC with millisecond precision", () => {
    const d = new Date("2026-04-21T14:30:45.123Z");
    expect(createSql("snowflake").literal(d).sql).toBe(
      "'2026-04-21 14:30:45.123+00:00'::TIMESTAMP_TZ",
    );
  });

  it("bigquery emits TIMESTAMP in UTC", () => {
    const d = new Date("2026-04-21T14:30:45.123Z");
    expect(createSql("bigquery").literal(d).sql).toBe(
      "TIMESTAMP '2026-04-21 14:30:45.123+00:00'",
    );
  });

  it("zero milliseconds still emit .000", () => {
    const d = new Date("2026-04-21T14:30:45.000Z");
    expect(createSql("snowflake").literal(d).sql).toBe(
      "'2026-04-21 14:30:45.000+00:00'::TIMESTAMP_TZ",
    );
  });

  it("cross-zone input normalized to UTC in output", () => {
    const d = new Date("2026-04-21T14:30:00-08:00"); // 22:30:00Z
    expect(createSql("snowflake").literal(d).sql).toBe(
      "'2026-04-21 22:30:00.000+00:00'::TIMESTAMP_TZ",
    );
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
npm test -- sql.test
```

Expected: fails — Date hits the `Cannot escape value of type object` branch.

- [ ] **Step 3: Implement Date branch**

Insert this block in `literal()` **before** the final `throw new TypeError`:

```typescript
    if (value instanceof Date) {
      if (Number.isNaN(value.getTime())) {
        throw new RangeError("Cannot escape invalid Date");
      }
      const yyyy = String(value.getUTCFullYear()).padStart(4, "0");
      const mm = String(value.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(value.getUTCDate()).padStart(2, "0");
      const HH = String(value.getUTCHours()).padStart(2, "0");
      const MM = String(value.getUTCMinutes()).padStart(2, "0");
      const SS = String(value.getUTCSeconds()).padStart(2, "0");
      const fff = String(value.getUTCMilliseconds()).padStart(3, "0");
      const iso = `${yyyy}-${mm}-${dd} ${HH}:${MM}:${SS}.${fff}+00:00`;
      if (dialect === "snowflake") {
        return makeSafe(`'${iso}'::TIMESTAMP_TZ`);
      }
      return makeSafe(`TIMESTAMP '${iso}'`);
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
npm test -- sql.test
```

Expected: Date tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/sql.ts tests/sql.test.ts
git commit -m "feat(sql): literal() for Date (always UTC instant)"
```

---

### Task 17: `sql.literal()` — arrays

**Files:**
- Modify: `src/sql.ts`
- Modify: `tests/sql.test.ts`

- [ ] **Step 1: Write the failing tests**

Append to `tests/sql.test.ts`:

```typescript
describe("literal - Array", () => {
  const sql = createSql("snowflake");

  it("non-empty", () => {
    expect(sql.literal([1, 2, 3]).sql).toBe("(1, 2, 3)");
  });

  it("mixed types", () => {
    expect(sql.literal([1, "a", null, true]).sql).toBe(
      "(1, 'a', NULL, TRUE)",
    );
  });

  it("empty → (NULL)", () => {
    expect(sql.literal([]).sql).toBe("(NULL)");
  });

  it("nested array throws", () => {
    expect(() => sql.literal([1, [2, 3]])).toThrow(TypeError);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
npm test -- sql.test
```

- [ ] **Step 3: Implement Array branch**

Insert this block in `literal()` **before** the Date branch (Arrays are also objects; ordering matters only in that we check `isSafeSql` first, which we already do):

```typescript
    if (Array.isArray(value)) {
      if (value.length === 0) {
        return makeSafe("(NULL)");
      }
      const parts = value.map((elem) => {
        if (Array.isArray(elem)) {
          throw new TypeError(
            "Nested arrays are not supported in SQL literals",
          );
        }
        return literal(elem).sql;
      });
      return makeSafe(`(${parts.join(", ")})`);
    }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
npm test -- sql.test
```

- [ ] **Step 5: Commit**

```bash
git add src/sql.ts tests/sql.test.ts
git commit -m "feat(sql): literal() for Array with IN-clause-safe empty behavior"
```

---

### Task 18: `sql.date()` with UTC-component regression

**Files:**
- Modify: `src/sql.ts`
- Modify: `tests/sql.test.ts`

- [ ] **Step 1: Write the failing tests**

Append to `tests/sql.test.ts`:

```typescript
describe("date", () => {
  it("from YYYY-MM-DD string (snowflake)", () => {
    expect(createSql("snowflake").date("2026-04-21").sql).toBe(
      "'2026-04-21'::DATE",
    );
  });

  it("from YYYY-MM-DD string (bigquery)", () => {
    expect(createSql("bigquery").date("2026-04-21").sql).toBe(
      "DATE '2026-04-21'",
    );
  });

  it("from Date uses UTC components", () => {
    const d = new Date("2026-04-21T23:00:00-08:00"); // UTC: 2026-04-22T07:00:00Z
    expect(createSql("snowflake").date(d).sql).toBe("'2026-04-22'::DATE");
  });

  it("from Date at midnight UTC", () => {
    const d = new Date("2026-04-21T00:00:00Z");
    expect(createSql("snowflake").date(d).sql).toBe("'2026-04-21'::DATE");
  });

  it("rejects malformed string", () => {
    expect(() => createSql("snowflake").date("2026/04/21")).toThrow(TypeError);
    expect(() => createSql("snowflake").date("not-a-date")).toThrow(TypeError);
  });

  it("rejects non-Date non-string", () => {
    // @ts-expect-error invalid input
    expect(() => createSql("snowflake").date(42)).toThrow(TypeError);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
npm test -- sql.test
```

Expected: `date() not implemented yet`.

- [ ] **Step 3: Implement `date`**

Add to `createSql` (below `literal`):

```typescript
  const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

  function date(value: Date | string): SafeSql {
    let iso: string;
    if (value instanceof Date) {
      if (Number.isNaN(value.getTime())) {
        throw new TypeError("date() received an invalid Date");
      }
      const yyyy = String(value.getUTCFullYear()).padStart(4, "0");
      const mm = String(value.getUTCMonth() + 1).padStart(2, "0");
      const dd = String(value.getUTCDate()).padStart(2, "0");
      iso = `${yyyy}-${mm}-${dd}`;
    } else if (typeof value === "string") {
      if (!DATE_RE.test(value)) {
        throw new TypeError(
          `date() expects Date or 'YYYY-MM-DD' string, got: ${JSON.stringify(value)}`,
        );
      }
      iso = value;
    } else {
      throw new TypeError(
        `date() expects Date or 'YYYY-MM-DD' string, got: ${typeof value}`,
      );
    }
    if (dialect === "snowflake") {
      return makeSafe(`'${iso}'::DATE`);
    }
    return makeSafe(`DATE '${iso}'`);
  }
```

Replace `api.date = ...` stub with `api.date = date;`.

- [ ] **Step 4: Run tests to verify they pass**

```bash
npm test -- sql.test
```

- [ ] **Step 5: Commit**

```bash
git add src/sql.ts tests/sql.test.ts
git commit -m "feat(sql): add sql.date() with UTC-component extraction"
```

---

### Task 19: Tagged template

**Files:**
- Modify: `src/sql.ts`
- Modify: `tests/sql.test.ts`

- [ ] **Step 1: Write the failing tests**

Append to `tests/sql.test.ts`:

```typescript
describe("sql tagged template", () => {
  const sql = createSql("snowflake");

  it("single interpolation escapes the value", () => {
    expect(sql`SET x = ${"O'Brien"}`).toBe("SET x = 'O''Brien'");
  });

  it("multiple interpolations", () => {
    const result = sql`SET a = ${1}, b = ${"two"}, c = ${null}`;
    expect(result).toBe("SET a = 1, b = 'two', c = NULL");
  });

  it("SafeSql passes through", () => {
    const table = sql.ident("in.c-main", "approvals");
    const q = sql`UPDATE ${table} SET x = ${"o'brien"}`;
    expect(q).toBe("UPDATE \"in.c-main\".\"approvals\" SET x = 'o''brien'");
  });

  it("empty template", () => {
    expect(sql``).toBe("");
  });

  it("end-to-end docs example", () => {
    const q = sql`UPDATE ${sql.ident("in.c-main", "approvals")} SET status = ${"approved"}, updated_at = ${sql.raw("CURRENT_TIMESTAMP")} WHERE id = ${123}`;
    expect(q).toBe(
      "UPDATE \"in.c-main\".\"approvals\" SET status = 'approved', updated_at = CURRENT_TIMESTAMP WHERE id = 123",
    );
  });

  it("backslash string round-trips via snowflake escape", () => {
    const s = "a\\nb"; // 4 chars: a, \, n, b
    expect(sql`x = ${s}`).toBe("x = 'a\\\\nb'");
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
npm test -- sql.test
```

Expected: `sql\`...\` not implemented yet`.

- [ ] **Step 3: Implement tag function**

Replace the `tag` function body in `createSql`:

```typescript
  function tag(strings: TemplateStringsArray, ...values: unknown[]): string {
    let result = strings[0] ?? "";
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      const piece = isSafeSql(v) ? v.sql : literal(v).sql;
      result += piece + (strings[i + 1] ?? "");
    }
    return result;
  }
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
npm test -- sql.test
```

- [ ] **Step 5: Commit**

```bash
git add src/sql.ts tests/sql.test.ts
git commit -m "feat(sql): tagged template returns escaped SQL string"
```

---

### Task 20: SafeSql passthrough + unknown-type + raw tests

**Files:**
- Modify: `tests/sql.test.ts` (implementation already handles from earlier tasks)

- [ ] **Step 1: Write the tests**

Append to `tests/sql.test.ts`:

```typescript
describe("SafeSql passthrough", () => {
  const sql = createSql("snowflake");

  it("literal returns the same SafeSql unchanged", () => {
    const marker = sql.raw("CURRENT_TIMESTAMP");
    expect(sql.literal(marker)).toBe(marker);
  });

  it("literal returns ident unchanged", () => {
    const i = sql.ident("in.c-main", "customers");
    expect(sql.literal(i)).toBe(i);
  });
});

describe("literal - unknown types", () => {
  const sql = createSql("snowflake");

  it("rejects plain object with hint", () => {
    expect(() => sql.literal({ a: 1 })).toThrow(/convert to string/);
  });

  it("rejects Symbol", () => {
    expect(() => sql.literal(Symbol("x"))).toThrow(TypeError);
  });

  it("rejects function", () => {
    expect(() => sql.literal(() => 1)).toThrow(TypeError);
  });
});

describe("raw", () => {
  const sql = createSql("snowflake");

  it("returns SafeSql with identical sql", () => {
    const r = sql.raw("CURRENT_TIMESTAMP");
    expect(r.__safe).toBe(true);
    expect(r.sql).toBe("CURRENT_TIMESTAMP");
  });

  it("rejects non-string", () => {
    // @ts-expect-error invalid input
    expect(() => sql.raw(123)).toThrow(TypeError);
  });

  it("passes through in tagged template", () => {
    expect(sql`ts = ${sql.raw("CURRENT_TIMESTAMP")}`).toBe("ts = CURRENT_TIMESTAMP");
  });
});
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
npm test -- sql.test
```

Expected: all pass — existing code handles each case.

- [ ] **Step 3: Commit**

```bash
git add tests/sql.test.ts
git commit -m "test(sql): cover SafeSql passthrough, unknown types, and raw()"
```

---

### Task 21: Export from `src/index.ts`

**Files:**
- Modify: `src/index.ts`
- Modify: `tests/sql.test.ts`

- [ ] **Step 1: Write the failing test**

Append to `tests/sql.test.ts`:

```typescript
describe("public package exports", () => {
  it("createSql, Dialect, SafeSql, Sql are re-exported from index", async () => {
    const pkg = await import("../src/index");
    expect(typeof pkg.createSql).toBe("function");
    // Dialect, SafeSql, Sql are types — import checks them at compile time.
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```bash
npm test -- sql.test
```

Expected: `pkg.createSql is undefined`.

- [ ] **Step 3: Add exports**

Modify `src/index.ts`. Append after the existing exports block:

```typescript
// SQL escape helper
export { createSql } from "./sql";
export type { Dialect, SafeSql, Sql } from "./sql";
```

- [ ] **Step 4: Run full suite + lint + typecheck**

```bash
npm run test:run
npm run lint
npm run typecheck
npm run build
```

Expected: all green.

- [ ] **Step 5: Commit + push**

```bash
git add src/index.ts tests/sql.test.ts
git commit -m "feat(sql): export createSql, Dialect, SafeSql, Sql from package root"
git push -u origin feat/sdk-quote-helper
```

**Phase B checkpoint.** Open a draft PR against `main` in the JS SDK repo. Do not merge yet — Phase C uses this branch.

---

## Phase C — Cross-verification and release prep (non-gating)

### Task 22: Byte-equality check for the docs example

**Files:**
- No code changes.

- [ ] **Step 1: Run the end-to-end docs example in both SDKs, capture output**

Python:
```bash
cd /Users/miroslavcillik/Projects/query-service-api-python-sdk
pytest tests/test_sql.py::TestFormat::test_end_to_end_docs_example -v
```

JS:
```bash
cd /Users/miroslavcillik/Projects/query-service-api-js-sdk
npm test -- sql.test -t "end-to-end"
```

- [ ] **Step 2: Verify the exact output string is identical in both**

Both should assert this string (one line, not the multi-line visual):

```
UPDATE "in.c-main"."approvals" SET status = 'approved', updated_at = CURRENT_TIMESTAMP WHERE id = 123
```

If they differ, fix the offending SDK and re-run both.

- [ ] **Step 3: No commit; this is a verification step only**

---

### Task 23: Manual Snowflake round-trip check (pre-release)

**Files:**
- No code changes. This is a human-in-the-loop verification per the spec's "Pre-release manual checklist."

- [ ] **Step 1: Use one SDK (either) to build a statement against a real Snowflake Storage Access workspace**

Run a mix of types: string with `'`, string with `\` (regression for backslash escape), date, tz-aware datetime, list for IN clause, `sql.raw("CURRENT_TIMESTAMP")`.

- [ ] **Step 2: Confirm the backslash-containing string round-trips byte-for-byte**

Insert `a\nb` (4 chars: a, \, n, b) via the helper, select it back, assert 4-char length. Catches any regression of the Snowflake backslash rule.

- [ ] **Step 3: Confirm the generated SQL appears correctly in query history**

Open the Snowflake query history or Keboola Storage Access query log and spot-check the emitted SQL for readability (no double-escaping, no stray characters).

- [ ] **Step 4: Document findings in the PR description**

No commit — append verification notes to each draft PR's description.

BigQuery round-trip is deferred until BQ Storage Access is generally available; noted in the spec as a followup.

---

### Task 24: Release preparation (optional, not gating)

**Files:**
- Modify: `pyproject.toml` (Python SDK) — bump version
- Modify: `src/keboola_query_service/_version.py` — bump version
- Modify: `package.json` (JS SDK) — bump version
- Modify: `src/client.ts` (JS SDK) — bump `VERSION` constant at line 45
- Modify: `README.md` in each repo — add a short "SQL escape helper" usage snippet

- [ ] **Step 1: Bump Python SDK version 0.2.0 → 0.3.0**

`pyproject.toml`:
```toml
version = "0.3.0"
```

`src/keboola_query_service/_version.py`:
```python
__version__ = "0.3.0"
```

Minor bump is correct because this is a purely additive API (new module + new exports, no behavior change to existing APIs).

- [ ] **Step 2: Bump JS SDK version 0.1.4 → 0.2.0**

`package.json`:
```json
"version": "0.2.0",
```

`src/client.ts` line 45:
```typescript
const VERSION = "0.2.0";
```

Minor bump for the same reason.

- [ ] **Step 3: Add a short README snippet in each repo**

Python `README.md` — add a new `## SQL escape helper` section near the end with:

````markdown
## SQL escape helper

```python
from keboola_query_service import SQL

sql = SQL("snowflake")
query = sql.format(
    "UPDATE {t} SET status = {status} WHERE id = {id}",
    t=sql.ident("in.c-main", "approvals"),
    status="approved",
    id=123,
)
client.execute_query(branch_id=..., workspace_id=..., statements=[query])
```
````

JS `README.md` — equivalent snippet using `createSql` + tagged template.

- [ ] **Step 4: Run full suite + lint + type-check in each repo**

Python:
```bash
pytest tests/ -v && ruff check src/ tests/ && mypy src/keboola_query_service/
```

JS:
```bash
npm run test:run && npm run lint && npm run typecheck && npm run build
```

Expected: all green.

- [ ] **Step 5: Commit in each repo**

Python:
```bash
git add pyproject.toml src/keboola_query_service/_version.py README.md
git commit -m "chore(release): bump to 0.3.0 for SQL escape helper"
git push
```

JS:
```bash
git add package.json src/client.ts README.md
git commit -m "chore(release): bump to 0.2.0 for SQL escape helper"
git push
```

- [ ] **Step 6: Mark draft PRs ready for review**

Do **not** merge or publish releases from this plan. Hand off the two PRs to the maintainers with the verification notes from Task 23 in the description. Releases follow the repos' existing PyPI / npm publish workflows after human sign-off.

---

## Self-review notes (done by plan author, not the engineer executing)

**Spec coverage check:**
- Dialect + `SQL`/`createSql` factory → Task 1 / 13
- `ident()` both dialects → Tasks 3–4 / 14
- `literal()` primitives → Task 5 / 15
- `literal()` strings with Snowflake `\` escape → Task 6 / 15 (combined in JS)
- `literal()` date/datetime → Task 7 (Python); JS `Date` always instant → Task 16
- `literal()` lists/arrays → Task 8 / 17
- `literal()` SafeSql passthrough + unknown types → Task 9 / 20
- `date()` with UTC components (JS) / `date()` helper (Python) → Task 10 / 18
- `raw()` → Task 2 (Python) / Task 13 (JS scaffold, tested in Task 20)
- Tagged template / `format` → Task 11 / 19
- Package exports → Task 12 / 21
- Cross-language docs example assertion → Task 22
- Pre-release manual checklist → Task 23
- Release prep → Task 24

All spec requirements mapped to tasks.

**Known trade-offs:**
- Phase A and Phase B are sequenced (Python first as reference). If two engineers are available they can parallelize, but the cross-SDK byte-equality check in Task 22 requires both complete.
- Task 9 and Task 20 are test-only (no implementation) because the preceding tasks' implementations already cover the cases. Retained as discrete tasks so the test coverage is explicit in the plan.
