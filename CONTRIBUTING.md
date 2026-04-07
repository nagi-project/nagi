# Contributing

## Development Setup

1. Install [mise](https://mise.jdx.dev) and [uv](https://docs.astral.sh/uv/)
2. Clone the repository
3. Run `mise run build` to create the virtual environment and build the package

## Running Checks

Before submitting a PR, run:

```console
$ mise run check
```

This runs formatting, linting (clippy), and tests.

## Project Structure

- `crates/nagi-core/` — Rust core
  - `interface` — Public API called from Python (evaluate, sync, init, etc.). Changes here affect the CLI surface.
  - `runtime` — Internal engine (compile, serve, storage, etc.). Not exposed to Python directly.
- `crates/nagi-schema-gen/` — Generates JSON Schema from Rust types
- `python/` — Python glue code (PyO3 bindings)
- `docs/` — Documentation (English and Japanese)

## Documentation

Resource reference pages include auto-generated schema sections. After changing resource definitions in Rust, run:

```console
$ mise run docs:gen-schema
```

This generates JSON Schema via `nagi-schema-gen` and updates the resource docs for both languages.

Documentation is maintained in English and Japanese under `docs/`. When modifying docs, apply the change to both languages.

## Pull Requests

- Create a feature branch from `main`
- Follow [Conventional Commits](https://www.conventionalcommits.org/) for commit messages
- Keep PRs focused — one concern per PR
