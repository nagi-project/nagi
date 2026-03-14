pub mod cron;
pub mod db;
pub mod dbt_profile;
pub mod duration;
pub mod evaluate;
pub mod kind;
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
    m.add_function(wrap_pyfunction!(py::read_cache, m)?)?;
    m.add_function(wrap_pyfunction!(py::list_cache, m)?)?;
    Ok(())
}
