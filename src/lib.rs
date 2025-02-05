mod gzeus;
mod py_gzeus;

use pyo3::{
    pymodule,
    types::{PyModule, PyModuleMethods},
    Bound, PyResult, Python,
};

#[pymodule]
#[pyo3(name = "_gzeus")]
fn gzcsv(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // m.add_class::<py_gzeus::PyCloudGzChunker>()?;
    m.add_class::<py_gzeus::PyGzChunker>()?;
    Ok(())
}
