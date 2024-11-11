use crate::gzcsv_core::{GzCsvChunker, GzCsvChunkerErr};
use flate2::bufread::GzDecoder;
use pyo3::exceptions::{PyFileExistsError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::fs::File;
use std::io::BufReader;

#[pyclass]
pub struct PyGzCsvFileReader {
    _chunker: GzCsvChunker<BufReader<File>>,
    _chunked_output: Vec<u8>,
    started: bool,
    finished: bool,
}

#[pymethods]
impl PyGzCsvFileReader {
    #[new]
    #[pyo3(signature = (path, buffer_size, separator=b'\n'))]
    fn new(path: &str, buffer_size: usize, separator: u8) -> PyResult<Self> {
        let file =
            File::open(path).map_err(|e| PyErr::new::<PyFileExistsError, _>(e.to_string()))?;

        let file_reader = BufReader::new(file);
        let gz: GzDecoder<BufReader<File>> = GzDecoder::new(file_reader);
        let chunker: GzCsvChunker<BufReader<File>> = GzCsvChunker::new(separator, buffer_size, gz);
        Ok(PyGzCsvFileReader {
            _chunker: chunker,
            _chunked_output: vec![0u8; buffer_size + buffer_size / 4],
            started: false,
            finished: false,
        })
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn read<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        if !self.started {
            self.started = true;
        }

        if self.finished {
            Ok(PyBytes::new_bound(py, &vec![]))
        } else {
            match self._chunker.read_and_write(&mut self._chunked_output) {
                Ok(n) => Ok(PyBytes::new_bound(py, &self._chunked_output[..n])),
                Err(e) => match e {
                    GzCsvChunkerErr::Finished => {
                        self.finished = true;
                        Ok(PyBytes::new_bound(py, &vec![]))
                    }
                    GzCsvChunkerErr::IoError(s) => Err(PyErr::new::<PyValueError, _>(s)),
                },
            }
        }
    }
}
