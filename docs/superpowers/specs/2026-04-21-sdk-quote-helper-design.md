# SDK SQL Quote Helper — Design

**Date:** 2026-04-21
**Scope:** `query-service-api-python-sdk` and `query-service-api-js-sdk`
**Driver:** PR #910 (`connection-docs`), review thread on `data-apps/storage-access/index.md:352` — reviewer flagged string-value SQL injection as a persistent footgun even with type coercion and allowlists

## Context

Keboola's Query Service accepts raw SQL strings. It does not support parameterized queries or server-side bind variables. This leaves SDK users to assemble SQL themselves, which the current Storage Access docs demonstrate with `f"... WHERE id = {int(user_input)}"` and allowlist-validated strings — approaches that review feedback on PR #910 called fragile, especially for string values.

A first-class escape/quote helper in both SDKs lets us close that gap cleanly. The helper produces safely-escaped SQL fragments from arbitrary values, covering the primitives (literals, identifiers) and a template convenience built on top.

## Goals

1. Users can build safe SQL strings without writing `f"...{user_input}..."` patterns.
2. The helper is dialect-aware from day one (Snowflake, BigQuery), with dialect bound once per app.
3. Covers the cases that appear in the Storage Access docs example: dynamic table/column names, dynamic literal values of mixed types, `IN (...)` lists.
4. Unsafe or ambiguous inputs raise clear exceptions rather than producing quietly-wrong SQL.

## Non-goals

- No SQL parser or AST. The helper escapes values, not statements.
- No dialect auto-detection from `Client` / manifest.
- No emulation of positional bind variables (`?`).
- No end-to-end tests against real Snowflake / BigQuery servers.
- No updates to `connection-docs` in this spec — that's a followup PR once both SDKs ship a release.
- No support for user-defined type registrations (can add later).
- No `bytes`, `Decimal`, `UUID` literal types in v1 — callers convert to `str` explicitly.

## Architecture

### Python SDK

New module `src/keboola_query_service/sql.py` containing:

- `Dialect = Literal["snowflake", "bigquery"]`
- `SafeSql` — frozen dataclass, single field `sql: str`, `__str__` returns `self.sql`. Trust marker for already-escaped fragments.
- `SQL` — factory class bound to a single dialect. Methods: `literal()`, `ident()`, `date()`, `raw()`, `format()`.

Exports added to `src/keboola_query_service/__init__.py`: `SQL`, `SafeSql`, `Dialect`.

### JS SDK

New module `src/sql.ts` containing:

- `Dialect = "snowflake" | "bigquery"`
- `SafeSql` — branded interface `{ readonly __safe: true; readonly sql: string }`
- `Sql` — the interface of what `createSql()` returns: a callable tag function with `.literal()`, `.ident()`, `.date()`, `.raw()` methods attached
- `createSql(dialect): Sql` — factory

Re-exported from `src/index.ts`: `createSql`, `Dialect`, `SafeSql`, `Sql` (the return type of `createSql`, useful in type annotations).

### Why a separate module, not methods on `Client`

The helper is pure — no I/O, no async, no httpx/fetch dependency. Keeping it separate from `Client`:

- Tests run synchronously in milliseconds, no HTTP mocks required.
- Users can build queries in contexts where no client has been constructed (unit tests, scripts).
- Failure modes are different (synchronous `TypeError` on bad input vs. async HTTP errors on the client).

### Why a factory rather than free functions

Dialect is a stable per-app choice. Binding it once removes `dialect=` noise at every call site.

## API surface

### Python

```python
from keboola_query_service import SQL

sql = SQL("snowflake")

# Primitives — return SafeSql
sql.literal(value)           # value: object
sql.ident(*parts)            # parts: str, at least one, not split on dots
sql.date(value)              # value: datetime.date | str "YYYY-MM-DD" — emits DATE literal explicitly
sql.raw(s)                   # s: str — wraps a pre-escaped string as SafeSql; reviewed escape hatch

# Template — returns plain str (a complete statement fragment)
sql.format(template, **values)
```

**Usage (the docs example):**

```python
query = sql.format(
    "UPDATE {t} SET status = {status}, updated_at = CURRENT_TIMESTAMP WHERE id = {id}",
    t=sql.ident("in.c-main", "approvals"),
    status="approved",
    id=123,
)
client.execute_query(branch_id=..., workspace_id=..., statements=[query])
```

**Standalone primitives also work outside `format`:**

```python
client.execute_query(
    ...,
    statements=[f"SELECT * FROM {sql.ident('in.c-main', 'customers')} LIMIT 10"],
)
```

### JS

```typescript
import { createSql } from "@keboola/query-service";

const sql = createSql("snowflake");

// Primitives
sql.literal(value);                    // unknown → SafeSql
sql.ident(...parts);                   // string[] → SafeSql
sql.date(value);                       // Date | "YYYY-MM-DD" → SafeSql emitting DATE literal
sql.raw(s);                            // string → SafeSql; reviewed escape hatch

// Tagged template — returns string
sql`...${value}...`;
```

**Usage:**

```typescript
const query = sql`UPDATE ${sql.ident("in.c-main", "approvals")} SET status = ${status}, updated_at = CURRENT_TIMESTAMP WHERE id = ${id}`;
await client.executeQuery({ branchId, workspaceId, statements: [query] });
```

### Why `format` / tagged template returns `str`, not `SafeSql`

The template output is a complete statement meant to be sent to the Query Service, not a fragment meant for further composition. Returning `str` signals terminality. Callers who need nested composition use `literal()` / `ident()` explicitly. If nested `format` composition becomes necessary later, adding `format_safe()` / a `raw` marker is non-breaking.

## Escape rules

### Identifiers — `ident(*parts)`

| Dialect | Quote char | Escape rule | Reject |
|---|---|---|---|
| `snowflake` | `"` | internal `"` → `""` | `\0` only |
| `bigquery` | `` ` `` | prefix internal `` ` `` and `\` with `\` | `\0`, `\n`, `\r` |

- Each part is quoted independently and joined with `.`.
- Dots inside a part are preserved, never split (so `"in.c-main"` stays one identifier).
- All other Unicode characters (including spaces, tabs, control chars except those listed) are permitted — quoted identifiers accept them in both dialects.
- Zero parts → `ValueError` (Python) / `TypeError` (JS).
- Empty-string part → `ValueError` (Python) / `TypeError` (JS).
- Non-string part (Python only) → `TypeError`.

### Literals — `literal(value)`

Dispatch order matters: check `SafeSql` first, then `bool` before `int` (because `isinstance(True, int)` is `True`).

| Input | Output (both dialects unless noted) |
|---|---|
| `SafeSql` / branded object | passed through unchanged (same identity) |
| `None` / `null` / `undefined` | `NULL` |
| `True` / `true` | `TRUE` |
| `False` / `false` | `FALSE` |
| `int` (Python arbitrary precision; JS `number` integer or `bigint`) | decimal repr |
| `float` / `number` — finite | Python `repr(v)`; JS `String(v)` (both round-trip-safe) |
| `float` / `number` — `NaN`, `±Infinity` | `ValueError` / `RangeError` |
| `str` | Both dialects: wrap in `'`, escape internal `'` → `''` AND `\` → `\\`. Reject `\0`. (Snowflake interprets backslash sequences in string literals just like BigQuery — `'a\nb'` means 3 chars on both.) |
| Python `datetime.date` | Snowflake `'YYYY-MM-DD'::DATE`, BQ `DATE 'YYYY-MM-DD'` |
| Python naive `datetime.datetime` | Snowflake `'YYYY-MM-DD HH:MM:SS.ffffff'::TIMESTAMP_NTZ`, BQ `DATETIME '...'` (6 fractional digits) |
| Python tz-aware `datetime.datetime` | Snowflake `'YYYY-MM-DD HH:MM:SS.ffffff±HH:MM'::TIMESTAMP_TZ`, BQ `TIMESTAMP '...'` (6 fractional digits) |
| JS `Date` (always an instant) | Snowflake `'YYYY-MM-DD HH:MM:SS.fff+00:00'::TIMESTAMP_TZ`, BQ `TIMESTAMP '...'` (3 fractional digits, emitted in UTC since `Date` carries no zone) |
| `datetime.time` alone | `TypeError` |
| non-empty `list` / tuple / Array | `(lit1, lit2, ...)` with each element recursively escaped |
| empty `list` / Array | `(NULL)` — `IN (NULL)` returns no rows; `IN ()` is syntax error |
| nested list / Array | `TypeError` |
| anything else | `TypeError` with message naming the type |

### Template layer

1. Walk interpolation values in order.
2. For each value: if `SafeSql`, append `value.sql` verbatim. Otherwise append `self.literal(value).sql`.
3. Concatenate with the literal string parts.
4. Return `str` / `string`.

**Python** uses a custom `string.Formatter` subclass that overrides `format_field` to route every interpolated value through the `SafeSql`-check → `literal()` pipeline:

```python
class _SafeFormatter(string.Formatter):
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

The template engine calls `vformat(template, (), kwargs)` — positional args tuple is empty, so `{0}` / `{}` raise `IndexError`. **Named placeholders only in v1.** Attribute (`{name.attr}`) and index (`{name[0]}`) references work — the resolved value still flows through `literal()`, so safety is preserved. Non-empty `format_spec` (e.g. `{p:.2f}`) raises `ValueError` to avoid silently dropping formatting intent. Literal braces escape as `{{` / `}}`.

**JS** uses a tagged template. Values are always positional by nature of the tag.

## Error handling

### Python exceptions (all stdlib, no new types)

| Condition | Exception | Message shape |
|---|---|---|
| Unknown type in `literal()` | `TypeError` | `"Cannot escape value of type {typename}. Supported: None, bool, int, float, str, date, datetime, list/tuple, SafeSql. If you have a Decimal/UUID/bytes value, convert to str explicitly and pass that."` |
| `NaN`/`±Infinity` | `ValueError` | `"Cannot escape non-finite float: {value!r}. Snowflake and BigQuery literals do not support NaN/Infinity."` |
| `ident()` zero parts | `ValueError` | `"ident() requires at least one part"` |
| `ident()` empty part | `ValueError` | `"ident() part must be a non-empty string, got: {part!r}"` |
| `ident()` non-string part | `TypeError` | same shape |
| `ident()` part contains rejected char | `ValueError` | `"ident() part contains {char_name}, which is not permitted in {dialect} identifiers"` (Snowflake: NUL only; BigQuery: NUL, newline, carriage return) |
| Literal string contains `\0` | `ValueError` | `"String literal contains NUL character, which neither Snowflake nor BigQuery accept"` |
| Nested list in literal | `TypeError` | `"Nested lists/tuples are not supported in SQL literals"` |
| `time` object passed as literal | `TypeError` | standard unknown-type message |
| Non-empty `format_spec` in `format()` | `ValueError` | `"Format spec {format_spec!r} is not supported in sql.format(). Pre-format the value before passing."` |
| Positional placeholder `{0}` / `{}` | `IndexError` | stdlib default (v1 is named-only) |
| Unknown named placeholder in `format()` | `KeyError` | stdlib default |
| `SQL()` invalid dialect | `ValueError` | `"Unknown dialect: {value!r}. Supported: 'snowflake', 'bigquery'"` |
| `date()` input is not `date`/str / is malformed `YYYY-MM-DD` | `ValueError` / `TypeError` | `"date() expects datetime.date or a 'YYYY-MM-DD' string, got: {value!r}"` |

### JS exceptions

Use built-in `TypeError` and `RangeError`. Same message text as Python where applicable. Unknown placeholders are N/A for JS — tagged templates are positional by construction.

## Edge cases locked in explicitly

1. **Empty lists** → `(NULL)`. `IN ()` is syntax error; `IN (NULL)` returns no rows, semantically the correct empty-set.
2. **`bool` dispatch before `int`** — must emit `TRUE`/`FALSE`, not `1`/`0`. Regression test required.
3. **`SafeSql` passed to `literal()`** → returned unchanged (same identity). Enables idempotent escaping.
4. **`format`/tag interpolation of `SafeSql`** → passthrough. This is the composition rule.
5. **Python format syntax** — `vformat` is called with empty positional args, so `{0}` / `{}` raise `IndexError`. Named placeholders (`{name}`) and dereference chains (`{name.attr}`, `{name[0]}`) work — resolved values always flow through `literal()`. Non-empty `format_spec` (`{p:.2f}`) raises `ValueError`. Safety does not depend on restricting placeholder syntax; it depends on every resolved value flowing through `literal()`.
6. **Datetime shape → SQL type mapping** — no UTC conversion in Python. Naive datetime → wall-clock type (`TIMESTAMP_NTZ` / `DATETIME`), tz-aware → instant type (`TIMESTAMP_TZ` / `TIMESTAMP`). Caller's Python representation decides the SQL type.
7. **JS `Date` is always an absolute instant** — emits the instant type in UTC (`Z` offset) with millisecond precision. JS `Date` has no "naive" form; users who need `DATE` literals call `sql.date(input)` explicitly with a `YYYY-MM-DD` string or a `Date`. **When `sql.date(Date)` is called, the calendar date is extracted via UTC components (`getUTCFullYear`/`getUTCMonth`/`getUTCDate`), matching the "always UTC" stance of `literal(Date)`.** Callers who want a specific local calendar date must pass a `YYYY-MM-DD` string.
8. **JS number dispatch** — `String(v)` is used for all finite numbers (integer and non-integer); no `Number.isInteger` branching. `String(42)` → `"42"`, `String(42.5)` → `"42.5"`, both valid SQL. `bigint` is allowed and also goes through `String(v)` (→ `"42"`, no `n` suffix).
9. **Float formatting** — `repr(0.1 + 0.2)` is `'0.30000000000000004'`. Faithful to the IEEE 754 double the caller passed; not rounded. Document in the `literal()` docstring so nobody's surprised. Scientific notation (e.g. `repr(1e300)` → `'1e+300'`) is accepted by both dialects; covered by a test.
10. **`SafeSql` escape hatch via `sql.raw(s)`** — use when you need to inject a SQL fragment that the helper doesn't directly support (e.g., `CURRENT_TIMESTAMP`, backend-specific function calls). The `SafeSql` dataclass constructor remains public (Python ergonomics), but `sql.raw()` is the reviewed, documented entry point — grep-friendly in code review. Docstring warns: use only with strings the caller fully controls (never user input).

## Testing

### Python — `tests/test_sql.py` (pytest, already a dev dep)

Pure-function tests, no HTTP mocks. Organized by class:

- `TestDialect` — valid constructs, `ValueError` on unknown.
- `TestIdentSnowflake` — single / multi-part, dots preserved, `"` doubled, spaces/Unicode allowed, reject empty / zero parts / `\0` / non-str.
- `TestIdentBigQuery` — same plus backtick escape, backslash escape, reject `\n`/`\r`.
- `TestLiteralPrimitives` — `None`, `True`, `False`, `int` (incl. large: `10**100`), `float` (including `0.1 + 0.2` round-trip lock-in, `1e300` → `'1e+300'` scientific notation), reject non-finite.
- `TestLiteralStrings` — empty, simple, embedded `'`, embedded `\` (both dialects emit `\\`), embedded newline (preserved in output as literal newline inside the quotes, since we escape `\` not the newline char itself), reject `\0`.
- `TestLiteralStringsSnowflakeBackslash` — regression: Python string `"a\\nb"` (4 chars: a, \, n, b) emitted as `'a\\nb'` (backslash escaped), which Snowflake parses back to 4 chars, not 3.
- `TestLiteralDates` — `date`, naive `datetime` (microsecond precision), tz-aware `datetime` (offset emitted), reject `time` alone.
- `TestLiteralLists` — non-empty, mixed types, empty → `(NULL)`, reject nested, tuple accepted.
- `TestLiteralBoolBeforeInt` — regression: `True` is `TRUE` not `1`.
- `TestLiteralUnknownTypes` — `Decimal`, `UUID`, `bytes`, custom class all raise `TypeError` with a message suggesting `str(value)` workaround.
- `TestDate` — `sql.date(date(2026, 4, 21))`, `sql.date("2026-04-21")`, reject malformed string, reject `datetime` (too specific, caller should pass `.date()` themselves).
- `TestRaw` — `sql.raw("CURRENT_TIMESTAMP")` → `SafeSql("CURRENT_TIMESTAMP")`; passes through `literal()` and `format()` unchanged.
- `TestSafeSqlPassthrough` — `literal(SafeSql(...))` and `literal(ident(...))` identity-preserved.
- `TestFormat` — single/multiple named placeholders, `SafeSql` values (passthrough), plain values (escaped), attribute (`{obj.attr}`) and index (`{obj[0]}`) references still escape the resolved value, positional `{0}` raises `IndexError`, non-empty `format_spec` (`{p:.2f}`) raises `ValueError`, unknown named placeholder → `KeyError`, `{{`/`}}` literal braces.
- `TestEndToEndDocsExample` — constructs the exact Storage Access UPDATE example, asserts exact output string. Regression test for "we can fix the doc."

### JS — `tests/sql.test.ts` (vitest, already configured)

Mirrors Python structure with `describe`/`it`. Additionally:

- `describe("literal - number")` — `String(v)` dispatch for int and float cases; `NaN`/`Infinity` → `RangeError`; `bigint` supported (`String(42n)` → `"42"`).
- `describe("literal - Date")` — always emits instant type in UTC with millisecond precision (`Date` has no zone). No DATE heuristic; `sql.date()` is the path for DATE literals.
- `describe("date")` — `sql.date(new Date(...))` and `sql.date("2026-04-21")`; reject malformed strings with `TypeError`. **Regression:** `sql.date(new Date("2026-04-21T23:00:00-08:00"))` → `DATE '2026-04-22'` (UTC components), *not* `DATE '2026-04-21'` (local components), matching `literal(Date)` behavior.
- `describe("raw")` — `sql.raw("CURRENT_TIMESTAMP")` → `SafeSql`; passthrough in tagged template.
- `describe("sql tagged template")` — Backslash in JS string literal (e.g. `"a\\nb"`) round-trips correctly via the new Snowflake escape rule.

### Explicitly not tested

- Round-trip against real Snowflake / BigQuery. Would need credentials and tolerate flakiness. Manual verification noted as a pre-release checklist item below rather than automated test.
- Performance / benchmarks. O(n) in output length; called once per statement.

### CI

Both SDKs run tests in CI on every PR (pytest + ruff for Python; vitest + eslint + tsc for JS). New files must satisfy existing lint/type configs. No CI config changes required.

## Pre-release manual checklist

Before tagging releases:

1. Run a single statement through a real Snowflake workspace using `sql.format(...)` with a mix of types (string with `'`, string with `\`, date, tz-aware datetime, list for IN clause). Confirm the backslash-containing string round-trips byte-for-byte (regression test for the Snowflake backslash escape rule).
2. Repeat against a BigQuery workspace once BQ Storage Access is available.
3. Verify the generated SQL appears correctly in the query history (readable, not double-escaped).

## Followups (not in this spec)

- **`connection-docs` PR** — update `data-apps/storage-access/index.md` to use the helper in the docs example. Blocks on both SDKs shipping a release with `SQL`/`createSql`.
- **Docs repo naming fix** — current storage-access doc references `keboola.query-service-client` but the actual PyPI package is `keboola-query-service`. Fix in the same follow-up PR.
- **Release notes / changelogs** — both SDK repos. Covered in the implementation plan.

## Open questions

None remaining from brainstorming. Ready for implementation planning.
