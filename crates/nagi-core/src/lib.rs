// Many items are only referenced from PyO3 bindings. Without the "python" feature
// (e.g. `cargo test`), these appear as dead code even though they are used at runtime.
#![cfg_attr(not(feature = "python"), allow(dead_code))]

pub mod interface;
pub(crate) mod runtime;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pymodule]
#[pyo3(name = "_nagi_core")]
fn nagi_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    interface::py::register(m)
}
