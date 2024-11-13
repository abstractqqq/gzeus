mod gzcsv_core;
mod py_gzcsv;

use pyo3::{
    pymodule,
    types::{PyModule, PyModuleMethods},
    Bound, PyResult, Python,
};

#[pymodule]
#[pyo3(name = "_gzcsv")]
fn gzcsv(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<py_gzcsv::PyCsvChunker>()?;
    m.add_class::<py_gzcsv::PyGzCsvChunker>()?;
    Ok(())
}
