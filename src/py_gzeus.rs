use crate::gzeus::{CsvChunker, ReaderErr};
use flate2::bufread::MultiGzDecoder;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::{fs::File, io::Read};

#[pyclass]
pub struct PyGzChunker {
    _chunker: CsvChunker,
    _reader: MultiGzDecoder<std::io::BufReader<File>>,
    _chunk_buffer: Vec<u8>,
    started: bool,
    finished: bool,
    n_reads: usize,
    bytes_decompressed: usize,
}

#[pymethods]
impl PyGzChunker {
    #[new]
    #[pyo3(signature = (path, buffer_size, new_line_symbol))]
    fn new(path: &str, buffer_size: usize, new_line_symbol: &str) -> PyResult<Self> {
        let file = File::open(path).map_err(PyErr::from)?;
        let file_reader = std::io::BufReader::with_capacity(buffer_size, file);
        let gz: MultiGzDecoder<std::io::BufReader<File>> = MultiGzDecoder::new(file_reader);
        Ok(Self {
            _chunker: CsvChunker::new(new_line_symbol),
            _reader: gz,
            _chunk_buffer: vec![0u8; buffer_size + 4096],
            started: false,
            finished: false,
            n_reads: 0,
            bytes_decompressed: 0,
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

    pub fn bytes_decompressed(&self) -> usize {
        self.bytes_decompressed
    }

    pub fn read_full<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        if !self.started {
            self.started = true;
        }

        match self._reader.read(&mut self._chunk_buffer) {
            Ok(n) => {
                self.n_reads += 1;
                self.bytes_decompressed += n;
                self.finished = true;
                // Safety:
                // Vec is contiguous, all u8s, and n is <= len()
                // PyBytes is also immutable, and is only used for reading
                Ok(unsafe { PyBytes::from_ptr(py, self._chunk_buffer.as_ptr(), n) })
            }
            Err(ioe) => Err(PyErr::from(ioe)),
        }
    }

    pub fn read_chunk<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        if !self.started {
            self.started = true;
        }

        if self.finished {
            self._chunk_buffer.clear();
            Ok(PyBytes::new(py, &[]))
        } else {
            match self
                ._chunker
                .read_and_write(&mut self._reader, &mut self._chunk_buffer)
            {
                Ok(n) => {
                    self.n_reads += 1;
                    self.bytes_decompressed += n;
                    // Safety:
                    // Vec is contiguous, all u8s, and n is <= len()
                    // PyBytes is also immutable, and is only used for reading
                    Ok(unsafe { PyBytes::from_ptr(py, self._chunk_buffer.as_ptr(), n) })
                }
                Err(e) => match e {
                    ReaderErr::Finished => {
                        self.finished = true;
                        self._chunk_buffer.clear();
                        Ok(PyBytes::new(py, &[]))
                    }
                    ReaderErr::IoError(ioe) => Err(PyErr::from(ioe)),
                    ReaderErr::Other(s) => Err(PyErr::new::<PyValueError, _>(s)),
                },
            }
        }
    }
}
