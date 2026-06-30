## Why I chose this issue:

I Chose Issue: #2766 bug: New setting alias schema for built-in default_target_schema because this issue aligns with my familiarity with both Python and SQL. The issue is labeled both Good First Issue, and Accepting Pull Requests meaning that it is still an issue that is open to be worked on and a good issue for a first-time contributor like me. It also has clearly outlined success criteria and is well defined.

This issue is a good fit for me because:

1. I work in SQL often at work and understand the core request of the issue
1. Python is the coding language that I am the most familiar with
1. the codebase seems to be clearly documented and easy to navigate

The issue is that when a user sets a schema in the MeltanoLabs variant of target-snowflake the default schema overrides it. I believe I can contribute an effective solution to the issue. I have left a comment introducing myself and stating that I would like to work on the issue

## Steps to Reproduce

1. If default_target_schema is set in config → use it.
1. Else, if the stream name looks like <schema>-<table> (or <db>-<schema>-<table>),
   i.e. it splits on - into 2 or 3 parts → use the second‑to‑last part as the schema name.
1. Else → None.

## Reproduction Evidence

https://github.com/Rasaec5/Meltano_sdk/tree/main

## Implementation Plan

### Solution Plan — meltano/sdk Issue #2766

**Goal:** Let SDK SQL targets honor a `schema` setting as an alias for `default_target_schema`,
without breaking existing stream-derived behavior.

#### Resolution order (target)

`default_target_schema` → `schema` → stream-derived (`<schema>-<table>`) → `None`.
Putting `default_target_schema` first keeps current behavior a no-op change for existing users.

#### Code change

In `singer_sdk/sinks/sql.py`, update `SQLSink.schema_name`:

```python
@property
def schema_name(self) -> str | None:
    default_target_schema = self.config.get("default_target_schema")
    schema = self.config.get("schema")
    parts = self.stream_name.split("-")
    if default_target_schema:
        return default_target_schema
    if schema:
        return schema
    return self.conform_name(parts[-2], "schema") if len(parts) in {2, 3} else None
```

#### Supporting changes

1. **Decide on `schema` precedence vs stream-derived.** As above, an explicit `schema` setting
   should win over the implicit stream prefix — that is the user's intent in the bug report.
1. **Builtin setting (optional).** Consider registering `schema` (and/or treating it as an alias
   of `default_target_schema`) in `SQLTarget`'s base config schema so all SQL targets get it
   consistently, rather than relying on each target to define it.
1. **Conform or not?** `default_target_schema` is returned verbatim; for consistency, return
   `schema` verbatim too (don't pass through `conform_name`), matching user expectations.

#### Tests

- Unit test `schema_name` for the matrix: only `schema`; only `default_target_schema`; both
  (latter wins); neither + `TAP_SCHEMA-users` (stream-derived); neither + single-part stream
  (`None`).
- Regression test: existing behavior unchanged when only `default_target_schema` is set.

#### Validation

- Run the §3 SDK-only repro from the reproduction guide and confirm `('TARGET_SCHEMA', 'users')`
  when only `schema` is set.
- Optionally run the target-postgres end-to-end repro.

#### Docs & release

- Document the new `schema`/`default_target_schema` relationship and precedence.
- Add a changelog entry; ship as a minor (behavior-additive) release.

## Implementation Notes

SDK-based SQL targets choose their destination schema in `SQLSink.schema_name`
(`singer_sdk/sql/sink.py`). Previously the order was: use `default_target_schema` if set,
otherwise derive the schema from the incoming stream ID when it looks like `<schema>-<table>`,
otherwise return `None`. This meant a target's own `schema` setting was silently ignored when
`default_target_schema` was unset — records for a stream like `TAP_SCHEMA-users` landed in a
`tap_schema` schema, which fails when the connecting role lacks privileges there.

Work completed:

- Added a `schema_config` cached property on `SQLSink` that reads the `schema` setting
  (mirroring the existing `default_target_schema` property) and returns it verbatim.
- Updated `SQLSink.schema_name` to fall back to `schema_config` between the
  `default_target_schema` check and the stream-derived logic. New resolution order:
  `default_target_schema` -> `schema` -> stream-derived -> `None`.
- Kept `default_target_schema` first so existing behavior is unchanged (non-breaking).
- Added unit tests covering the new behavior.

## Code Changes

Draft Pull Request

Title: `fix(targets): honor 'schema' setting as alias for default_target_schema`

What: When a SQL target sets `schema` but not `default_target_schema`, the SDK now uses the
configured `schema` as the destination schema instead of deriving one from the incoming stream
ID.

Why: Closes #2766. Many SQL targets expose a `schema` connection setting. Today, if
`default_target_schema` is unset, `SQLSink.schema_name` derives the schema from the stream ID
(e.g. `TAP_SCHEMA-users` -> `tap_schema`). When the connecting account has no privileges on that
derived schema, schema/table/file-format creation fails. Users reasonably expect their
configured `schema` to be honored.

How:

- Added `SQLSink.schema_config` cached property reading the `schema` setting.
- `schema_name` falls back to it before the stream-derived branch.
- `default_target_schema` retains precedence, so the change is non-breaking.

Files changed:

- `singer_sdk/sql/sink.py` — add `schema_config`; update `schema_name`.
- `tests/sql/test_sink.py` — add `test_schema_name_schema_setting` parametrized cases.
- `tests/sql/test_schema_alias.py` — new dedicated test module for the alias behavior.

Backwards compatibility: No behavior change when `default_target_schema` is set, or when neither
setting is present (stream-derived behavior preserved). Only the previously-ignored `schema`
setting now takes effect.

## Testing Strategy

The schema-resolution logic is pure and deterministic, so it is validated without a live database.

- Unit tests (no external credentials): tests instantiate a dummy SQL connector/sink/target
  (SQLite URL, never actually connecting) and assert on `schema_name` / `schema_config` /
  `table_name`.
- Resolution-order matrix: `schema` overrides the stream-derived schema for single-, two-, and
  three-part stream names; `default_target_schema` takes precedence when both are set; only
  `default_target_schema` set (unchanged behavior); neither set (stream-derived fallback,
  including `None` for one- and four-part stream names).
- Edge cases: empty-string `schema` is falsy and falls through; `schema` is returned verbatim
  (not name-conformed); the `schema` setting does not affect table-name derivation.
- Regression safety: existing `tests/sql/test_sink.py` cases for `default_target_schema` and
  stream-derived names continue to pass.

How to run:

```
python -m pytest tests/sql/test_schema_alias.py tests/sql/test_sink.py -v
nox -s tests
nox -t typing
```

Result: `tests/sql/test_sink.py` -> 13 passed; `tests/sql/test_schema_alias.py` -> 13 passed.
