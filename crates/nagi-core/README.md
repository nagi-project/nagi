# nagi-core

Rust core library for Nagi, exposed to Python via PyO3 as the `nagi_cli._nagi_core` module.

## Module structure

- `interface` -- Public API surface. CLI-facing functions exposed to Python via PyO3.
- `runtime` -- `pub(crate)` internal modules. Not directly accessible from Python.

## Build

```bash
cargo build
cargo test
```

When building as a Python extension (via maturin), enable the `python` feature to compile the PyO3 bindings.
