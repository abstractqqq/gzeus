use crate::gzcsv_core::{CsvChunker, ReaderErr};
use flate2::bufread::GzDecoder;
use pyo3::exceptions::{PyFileExistsError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::fs::File;
use std::io::BufReader;

// There is no way to use a generic trait in a struct that we wish to use as a pyclass later.
// Therefore, code here is quite redundant.
// The Python classes here are single-purpose only, and will be managed by a higher level
// class in Python for ease of use.

// Strictly speaking, if data is a slice, and the creation of dataframe copies, then data doesn't
// need to be copied when being passed to Python. However, there is no way to pass a slice (&[u8]) to Python
// and using PyBytes incurs an additional copy. I might need to research more into this.

#[pyclass]
pub struct PyGzCsvChunker {
    _chunker: CsvChunker,
    _chunk_buffer: Vec<u8>,
    _reader: GzDecoder<BufReader<File>>,
    started: bool,
    finished: bool,
    n_reads: usize,
}

#[pymethods]
impl PyGzCsvChunker {
    #[new]
    #[pyo3(signature = (path, buffer_size, line_change_symbol))]
    fn new(path: &str, buffer_size: usize, line_change_symbol: Vec<u8>) -> PyResult<Self> {
        let file =
            File::open(path).map_err(|e| PyErr::new::<PyFileExistsError, _>(e.to_string()))?;

        let file_reader = BufReader::with_capacity(buffer_size, file);
        let gz: GzDecoder<BufReader<File>> = GzDecoder::new(file_reader);

        let b = line_change_symbol[0]; // by default bytes are translated to Vec<u8>

        Ok(Self {
            _chunker: CsvChunker::new(b),
            _chunk_buffer: vec![0u8; buffer_size + buffer_size / 4],
            _reader: gz,
            started: false,
            finished: false,
            n_reads: 0,
        })
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn has_started(&self) -> bool {
        self.started
    }

    pub fn n_reads(&self) -> usize {
        self.n_reads
    }

    pub fn read_chunk<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        if !self.started {
            self.started = true;
        }

        if self.finished {
            Ok(PyBytes::new_bound(py, &vec![]))
        } else {
            match self
                ._chunker
                .read_and_write(&mut self._reader, &mut self._chunk_buffer)
            {
                Ok(n) => {
                    self.n_reads += 1;
                    Ok(PyBytes::new_bound(py, &self._chunk_buffer[..n]))
                }
                Err(e) => match e {
                    ReaderErr::Finished => {
                        self.finished = true;
                        Ok(PyBytes::new_bound(py, &vec![]))
                    }
                    ReaderErr::IoError(s) => Err(PyErr::new::<PyValueError, _>(s)),
                },
            }
        }
    }
}

#[pyclass]
pub struct PyCsvChunker {
    _chunker: CsvChunker,
    _chunk_buffer: Vec<u8>,
    _reader: BufReader<File>,
    started: bool,
    finished: bool,
    n_reads: usize,
}

#[pymethods]
impl PyCsvChunker {
    #[new]
    #[pyo3(signature = (path, buffer_size, line_change_symbol))]
    fn new(path: &str, buffer_size: usize, line_change_symbol: Vec<u8>) -> PyResult<Self> {
        let file =
            File::open(path).map_err(|e| PyErr::new::<PyFileExistsError, _>(e.to_string()))?;

        let reader = BufReader::with_capacity(buffer_size, file);
        let b = line_change_symbol[0]; // by default bytes are translated to Vec<u8>
        Ok(Self {
            _chunker: CsvChunker::new(b),
            _chunk_buffer: vec![0u8; buffer_size + buffer_size / 4],
            _reader: reader,
            started: false,
            finished: false,
            n_reads: 0,
        })
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub fn has_started(&self) -> bool {
        self.started
    }

    pub fn n_reads(&self) -> usize {
        self.n_reads
    }

    pub fn read_chunk<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        if !self.started {
            self.started = true;
        }

        if self.finished {
            Ok(PyBytes::new_bound(py, &vec![]))
        } else {
            match self
                ._chunker
                .read_and_write(&mut self._reader, &mut self._chunk_buffer)
            {
                Ok(n) => {
                    self.n_reads += 1;
                    Ok(PyBytes::new_bound(py, &self._chunk_buffer[..n]))
                }
                Err(e) => match e {
                    ReaderErr::Finished => {
                        self.finished = true;
                        Ok(PyBytes::new_bound(py, &vec![]))
                    }
                    ReaderErr::IoError(s) => Err(PyErr::new::<PyValueError, _>(s)),
                },
            }
        }
    }
}
