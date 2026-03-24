# CLAUDE.md

## Project Overview

Nagi is a reconciliation engine that declaratively defines the desired state of data and automatically converges toward that state. It applies the same concept as the Kubernetes Reconciliation Loop to the data domain.

See `project_spec.md` for the full specification. All design-related information must be written in `project_spec.md`, not in this file.
Note: `project_spec.md` is gitignored. When committing spec-related changes, the spec file itself will not appear in `git status`.

## Environment Management

Tools are managed by mise (`mise.toml`). Python and dependencies are managed by uv (`pyproject.toml`).

- **mise** — Do not add `python` to mise; Python is managed by uv.
- **uv** — manages Python version, venv, dependencies, and maturin build.
- **maturin** — declared in `[build-system]` of `pyproject.toml`. Installed automatically by `uv sync`.

Build and development commands must be defined as mise tasks in `mise.toml` to ensure reproducibility across sessions.

## Build and Test

```bash
# Build Python package (creates .venv, installs dependencies, runs maturin)
mise run build   # = uv sync

# Run CLI
uv run nagi --help

# Build and test Rust core
cargo build
cargo test

# Run all checks (fmt, clippy, test)
mise run check
```

## Coding Conventions

### Rust

- Only use crates with significant community adoption (e.g. dtolnay crates, tokio ecosystem, serde ecosystem). Do not add community-maintained crates without clear evidence of wide adoption.
- Define error types with `thiserror`. Use `anyhow` only in binary crates.
- At PyO3 boundaries, convert Rust errors to `PyErr`. Never let panics leak into Python.
- Write doc comments only where behavior, constraints, or defaults are non-obvious from the name alone. Do not comment what the code already says.
- Comments in English.
- Use the Rust 2018 module style: `src/foo.rs` + `src/foo/bar.rs` instead of `src/foo/mod.rs`.

### Python

- Keep Python-side logic minimal. It should be glue code that calls into the Rust core.
- If business logic is leaking into Python, question the design.

### Security

- Never store credentials (access tokens, JWTs, private keys, client secrets) in struct fields, static variables, or any heap location that outlives the operation that requires them. Acquire credentials immediately before use and let them drop at the end of the enclosing scope.
- Never log, print, or include credentials in error messages, debug output, or serialized data.

### Testing

- When multiple test cases share the same structure (same setup, same assertion, different inputs/expected values), consolidate them so that each case runs independently and failures identify the exact case.
- Tests with distinct setup or assertion logic should remain as individual functions — do not force them into a parameterized form.
- **Rust:** Use a `macro_rules!` macro to generate individual `#[test]` functions per case. Do not add external parameterized testing crates (`rstest`, `test-case`, etc.).
- **Python:** Use `pytest.mark.parametrize`. Each case should have an `id` for readable test output (`pytest.param(..., id="name")`).

Rust macro example:

```rust
macro_rules! parse_duration_test {
    ($($name:ident: $input:expr => $secs:expr;)*) => {
        $(
            #[test]
            fn $name() {
                let w: Wrapper = serde_yaml::from_str($input).unwrap();
                assert_eq!(w.d.as_std(), StdDuration::from_secs($secs));
            }
        )*
    };
}

parse_duration_test! {
    parse_hours: "d: 24h" => 24 * 3600;
    parse_minutes: "d: 30m" => 30 * 60;
}
```

### Documentation

- All headings in documentation files (`docs/`) must be in English.

### General

- When using external tools, libraries, or SDKs, always refer to the official documentation to verify correct usage.
- Commit messages in English, Conventional Commits format (`feat:`, `fix:`, `refactor:`, etc.)
- Do not add `Co-Authored-By` trailers to commits.
- Development must follow TDD (test-driven development): write tests first, then implement behavior and make tests pass.
- Always run `mise run check` before committing. Do not commit if fmt, clippy, or test fails.

## Communication Rules

- Never include reactions or commentary like "Got it", "Nice", "Great", "Perfect", "Excellent", or any similar praise
- Do not evaluate or comment on the user's decisions or approach
- Report only facts and results concisely
- No compliments, encouragement, or expressions of empathy
- No unnecessary preambles or closing remarks
