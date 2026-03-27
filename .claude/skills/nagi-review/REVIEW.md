# Review Checklist

## Security (must pass)

- No credentials (tokens, keys, secrets) stored in struct fields, static variables, or long-lived heap locations
- No credentials logged, printed, or included in error messages
- No hardcoded secrets or sensitive values in source code
- PyO3 boundaries: all Rust errors converted to `PyErr`, no panics leak into Python

## Design

- Each function has a single responsibility. Specifically:
    - A function should not generate multiple kinds of resources (e.g. Asset and Sync in the same function)
    - If a function returns a Vec containing mixed resource kinds, consider splitting it
    - Side-channel information (e.g. "was X needed?") should be returned explicitly (bool, enum), not hidden inside a collection
- User-facing names (resource names, field names) must be self-explanatory to data engineers. Avoid programmer jargon (e.g. "noop") in names that appear in YAML or CLI output
- References between resources are validated at compile time. A dangling reference (e.g. Asset referencing a non-existent Connection or upstream) must produce an error, not silently resolve to None

## Correctness

- Error types defined with `thiserror` (not `anyhow` in library crates)
- No `unwrap()` or `expect()` on fallible operations in non-test code without justification
- Edge cases handled: empty inputs, boundary values, concurrent access
- New public APIs have corresponding tests

## Consistency

- Rust 2018 module style (`foo.rs` + `foo/bar.rs`, no `mod.rs`)
- Only widely-adopted crates (dtolnq, tokio, serde ecosystems)
- Python-side logic is minimal glue code; business logic stays in Rust
- Commit messages: Conventional Commits format, English

## Testing

- New behavior has tests written first (TDD)
- Parameterized tests use `macro_rules!` (Rust) or `pytest.mark.parametrize` (Python)
- No external parameterized testing crates (`rstest`, `test-case`)

## Skip

- Auto-generated files under `docs/schemas/`
- Lock file formatting changes
- `project_spec.md` (gitignored)
