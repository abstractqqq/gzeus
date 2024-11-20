use crate::gzcsv_core::{CsvChunker, ReaderErr};
use flate2::bufread::GzDecoder;
use pyo3::exceptions::{PyFileExistsError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::fs::File;

use async_compression::tokio::bufread::GzipDecoder;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

// There is no way to use a generic trait in a struct that we wish to use as a pyclass later.
// Therefore, code here is quite redundant.
// The exact reason is that the state of the reader needs to be preserved. But we cannot
// use (trait + monomorphization) to implement this struct.
// The Python classes here are single-purpose only, and will be managed by a higher level
// class in Python for ease of use.
// Strictly speaking, if data is a slice, and the creation of dataframe copies, then data doesn't
// need to be copied when being passed to Python. However, there is no way to pass a slice (&[u8]) to Python
// and using PyBytes incurs an additional copy. I might need to research more into this.

// There might be a workaround: pass in a NumPy uint8 array, which is allocated in Python,
// and mutate it (maybe unsafely), return the array which shouldn't result in any copy.
// Then use the ctype library to read the bytes (from the NumPy array) which should not copy.

#[pyclass]
pub struct PyGzCsvChunker {
    _chunker: CsvChunker,
    _reader: GzDecoder<std::io::BufReader<File>>,
    _chunk_buffer: Vec<u8>,
    started: bool,
    finished: bool,
    n_reads: usize,
}

#[pymethods]
impl PyGzCsvChunker {
    #[new]
    #[pyo3(signature = (path, buffer_size, line_change_symbol))]
    fn new<'py>(path: &str, buffer_size: usize, line_change_symbol: Vec<u8>) -> PyResult<Self> {
        let file =
            File::open(path).map_err(|e| PyErr::new::<PyFileExistsError, _>(e.to_string()))?;

        let file_reader = std::io::BufReader::with_capacity(buffer_size, file);

        let gz: GzDecoder<std::io::BufReader<File>> = GzDecoder::new(file_reader);
        let b = line_change_symbol[0]; // by default bytes are translated to Vec<u8>
        Ok(Self {
            _chunker: CsvChunker::new(b),
            _reader: gz,
            _chunk_buffer: vec![0u8; buffer_size + buffer_size / 4],
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
            self._chunk_buffer.clear();
            Ok(PyBytes::new(py, &[]))
        } else {
            match self
                ._chunker
                .read_and_write(&mut self._reader, &mut self._chunk_buffer)
            {
                Ok(n) => {
                    self.n_reads += 1;
                    Ok(PyBytes::new(py, &self._chunk_buffer[..n]))
                }
                Err(e) => match e {
                    ReaderErr::Finished => {
                        self.finished = true;
                        self._chunk_buffer.clear();
                        Ok(PyBytes::new(py, &[]))
                    }
                    ReaderErr::IoError(s) => Err(PyErr::new::<PyValueError, _>(s)),
                },
            }
        }
    }
}

#[pyclass]
pub struct PyS3GzCsvChunker {
    _chunker: CsvChunker,
    _reader: Mutex<GzipDecoder<object_store::buffered::BufReader>>,
    _chunk_buffer: Vec<u8>,
    _async_rt: Runtime,
    started: bool,
    finished: bool,
    n_reads: usize,
}

impl PyS3GzCsvChunker {
    async fn get_s3_bufreader(
        bucket: &str,
        path: &str,
        buffer_size: usize,
    ) -> PyResult<object_store::buffered::BufReader> {
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .map_err(|e| PyErr::new::<PyFileExistsError, _>(e.to_string()))?;

        let _path = Path::from(path);
        let data_future = store.get(&_path);

        let data_result = data_future
            .await
            .map_err(|e| PyErr::new::<PyFileExistsError, _>(e.to_string()))?;

        let meta = data_result.meta;
        // let bufread = object_store::buffered::BufReader::with_capacity(Arc::new(store), &meta, buffer_size);
        // let pinned_reader = Box::pin(bufread);

        Ok(object_store::buffered::BufReader::with_capacity(
            Arc::new(store),
            &meta,
            buffer_size,
        ))
    }
}

#[pymethods]
impl PyS3GzCsvChunker {
    #[new]
    #[pyo3(signature = (bucket, path, buffer_size, line_change_symbol))]
    fn new<'py>(
        bucket: &str,
        path: &str,
        buffer_size: usize,
        line_change_symbol: Vec<u8>,
    ) -> PyResult<Self> {
        let rt = Runtime::new().map_err(|e| PyErr::new::<PyFileExistsError, _>(e.to_string()))?;
        let reader = rt.block_on(Self::get_s3_bufreader(bucket, path, buffer_size))?;
        let b = line_change_symbol[0]; // by default bytes are translated to Vec<u8>
        let gz = GzipDecoder::new(reader);
        Ok(Self {
            _chunker: CsvChunker::new(b),
            _reader: Mutex::new(gz),
            _chunk_buffer: vec![0u8; buffer_size + buffer_size / 4],
            _async_rt: rt,
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
            self._chunk_buffer.clear();
            Ok(PyBytes::new(py, &[]))
        } else {
            let reader = self
                ._reader
                .get_mut()
                .map_err(|e| PyErr::new::<PyFileExistsError, _>(e.to_string()))?;

            let read_result = self._async_rt.block_on(
                self._chunker
                    .async_read_and_write(reader, &mut self._chunk_buffer),
            );

            match read_result {
                Ok(n) => {
                    self.n_reads += 1;
                    Ok(PyBytes::new(py, &self._chunk_buffer[..n]))
                }
                Err(e) => match e {
                    ReaderErr::Finished => {
                        self.finished = true;
                        self._chunk_buffer.clear();
                        Ok(PyBytes::new(py, &[]))
                    }
                    ReaderErr::IoError(s) => Err(PyErr::new::<PyValueError, _>(s)),
                },
            }
        }
    }
}
