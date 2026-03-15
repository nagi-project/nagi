pub mod compile;
pub mod cron;
pub mod db;
pub mod dbt;
pub mod duration;
pub mod evaluate;
pub mod kind;
pub mod select;
pub mod storage;

#[cfg(feature = "python")]
mod py;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pymodule]
#[pyo3(name = "_nagi_core")]
fn nagi_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py::parse_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(py::load_dbt_profiles, m)?)?;
    m.add_function(wrap_pyfunction!(py::load_dbt_profiles_from, m)?)?;
    m.add_function(wrap_pyfunction!(py::test_connection, m)?)?;
    m.add_function(wrap_pyfunction!(py::evaluate_asset, m)?)?;
    m.add_function(wrap_pyfunction!(py::dry_run_asset, m)?)?;
    m.add_function(wrap_pyfunction!(py::compile_assets, m)?)?;
    m.add_function(wrap_pyfunction!(py::list_dbt_origins, m)?)?;
    m.add_function(wrap_pyfunction!(py::select_assets, m)?)?;
    m.add_function(wrap_pyfunction!(py::read_cache, m)?)?;
    m.add_function(wrap_pyfunction!(py::list_cache, m)?)?;
    Ok(())
}
