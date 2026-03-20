pub mod compile;
pub mod config;
pub mod cron;
pub mod db;
pub mod dbt;
pub mod duration;
pub mod evaluate;
pub mod init;
pub mod kind;
pub mod log;
pub mod select;
pub mod serve;
pub mod status;
pub mod storage;
pub mod sync;

#[cfg(feature = "python")]
mod py;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
#[pymodule]
#[pyo3(name = "_nagi_core")]
fn nagi_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py::load_dbt_profiles, m)?)?;
    m.add_function(wrap_pyfunction!(py::evaluate_all, m)?)?;
    m.add_function(wrap_pyfunction!(py::compile_assets, m)?)?;
    m.add_function(wrap_pyfunction!(py::list_dbt_origin_dirs, m)?)?;
    m.add_function(wrap_pyfunction!(py::propose_sync, m)?)?;
    m.add_function(wrap_pyfunction!(py::execute_sync_proposal, m)?)?;
    m.add_function(wrap_pyfunction!(py::asset_status, m)?)?;
    m.add_function(wrap_pyfunction!(py::init_workspace, m)?)?;
    m.add_function(wrap_pyfunction!(py::run_dbt_debug, m)?)?;
    m.add_function(wrap_pyfunction!(py::write_init_dbt_files, m)?)?;
    m.add_function(wrap_pyfunction!(py::serve, m)?)?;
    m.add_function(wrap_pyfunction!(py::serve_resume, m)?)?;
    Ok(())
}
