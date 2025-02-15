use crate::gzeus::{CsvChunker, ReaderErr};
use flate2::bufread::GzDecoder;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::{fs::File, io::Read};

#[pyclass]
pub struct PyGzChunker {
    _chunker: CsvChunker,
    _reader: GzDecoder<std::io::BufReader<File>>,
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
        let gz: GzDecoder<std::io::BufReader<File>> = GzDecoder::new(file_reader);
        Ok(Self {
            _chunker: CsvChunker::new(new_line_symbol),
            _reader: gz,
            _chunk_buffer: vec![0u8; buffer_size + 50_000],
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

// #[pyclass]
// pub struct PyCloudGzChunker {
//     _chunker: CsvChunker,
//     _reader: Mutex<GzipDecoder<object_store::buffered::BufReader>>,
//     _chunk_buffer: Vec<u8>,
//     started: bool,
//     finished: bool,
//     n_reads: usize,
//     bytes_decompressed: usize,
// }

// // Todo: add config options

// impl PyCloudGzChunker {
//     async fn get_bufreader(
//         bucket: &str,
//         path: &Path,
//         provider: &str,
//         region: &str, // only for s3 rn. Need more specialized configs for each later.
//         buffer_size: usize,
//     ) -> PyResult<object_store::buffered::BufReader> {
//         let store = match provider.to_lowercase().as_ref() {
//             "aws" => Self::get_s3_object_store(bucket, region),
//             "gcp" => Self::get_gcs_object_store(bucket),
//             "azure" => Self::get_azure_object_store(bucket),
//             _ => Err(PyErr::new::<PyValueError, _>("Unknown clound provider.")),
//         }?;

//         let data_result = store
//             .get(path)
//             .await
//             .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;

//         let meta = data_result.meta;
//         Ok(object_store::buffered::BufReader::with_capacity(
//             store,
//             &meta,
//             buffer_size,
//         ))
//     }

//     fn get_s3_object_store(bucket: &str, region: &str) -> PyResult<Arc<dyn ObjectStore>> {
//         let store = AmazonS3Builder::from_env()
//             .with_bucket_name(bucket)
//             .with_region(region)
//             .build()
//             .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;

//         Ok(Arc::new(store))
//     }

//     fn get_gcs_object_store(bucket: &str) -> PyResult<Arc<dyn ObjectStore>> {
//         let store = GoogleCloudStorageBuilder::from_env()
//             .with_bucket_name(bucket)
//             .build()
//             .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;

//         Ok(Arc::new(store))
//     }

//     fn get_azure_object_store(container: &str) -> PyResult<Arc<dyn ObjectStore>> {
//         let store = MicrosoftAzureBuilder::from_env()
//             .with_container_name(container)
//             .build()
//             .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;

//         Ok(Arc::new(store))
//     }
// }

// #[pymethods]
// impl PyCloudGzChunker {
//     #[new]
//     #[pyo3(signature = (bucket, path, provider, region, buffer_size, new_line_symbol))]
//     fn new(
//         bucket: &str,
//         path: &str,
//         provider: &str,
//         region: &str,
//         buffer_size: usize,
//         new_line_symbol: &str,
//     ) -> PyResult<Self> {
//         let path = Path::from(path);
//         let reader = tokio::task::block_in_place(|| {
//             RUNTIME
//                 .block_on(Self::get_bufreader(
//                     bucket,
//                     &path,
//                     provider,
//                     region,
//                     buffer_size,
//                 ))
//                 .map(|br| Mutex::new(GzipDecoder::new(br)))
//         })?;

//         Ok(Self {
//             _chunker: CsvChunker::new(new_line_symbol),
//             _reader: reader,
//             _chunk_buffer: vec![0u8; buffer_size + 50_000],
//             started: false,
//             finished: false,
//             n_reads: 0,
//             bytes_decompressed: 0,
//         })
//     }

//     pub fn is_finished(&self) -> bool {
//         self.finished
//     }

//     pub fn has_started(&self) -> bool {
//         self.started
//     }

//     pub fn n_reads(&self) -> usize {
//         self.n_reads
//     }

//     pub fn bytes_decompressed(&self) -> usize {
//         self.bytes_decompressed
//     }

//     pub fn read_full<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
//         if !self.started {
//             self.started = true;
//         }

//         let reader = self
//             ._reader
//             .get_mut()
//             .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;

//         let read_result = RUNTIME.block_on(async { reader.read(&mut self._chunk_buffer).await });

//         match read_result {
//             Ok(n) => {
//                 self.n_reads += 1;
//                 self.bytes_decompressed += n;
//                 self.finished = true;
//                 // Safety:
//                 // Vec is contiguous, all u8s, and n is <= len()
//                 // PyBytes is also immutable, and is only used for reading
//                 Ok(unsafe { PyBytes::from_ptr(py, self._chunk_buffer.as_ptr(), n) })
//             }
//             Err(ioe) => Err(PyErr::from(ioe)),
//         }
//     }

//     pub fn read_chunk<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
//         if !self.started {
//             self.started = true;
//         }

//         if self.finished {
//             self._chunk_buffer.clear();
//             Ok(PyBytes::new(py, &[]))
//         } else {
//             let reader = self
//                 ._reader
//                 .get_mut()
//                 .map_err(|e| PyErr::new::<PyValueError, _>(e.to_string()))?;

//             let read_result = RUNTIME.block_on(
//                 self._chunker
//                     .async_read_and_write(reader, &mut self._chunk_buffer),
//             );

//             match read_result {
//                 Ok(n) => {
//                     self.n_reads += 1;
//                     self.bytes_decompressed += n;
//                     // Safety:
//                     // Vec is contiguous, all u8s, and n is <= len()
//                     // PyBytes is also immutable, and is only used for reading
//                     Ok(unsafe { PyBytes::from_ptr(py, self._chunk_buffer.as_ptr(), n) })
//                 }
//                 Err(e) => match e {
//                     ReaderErr::Finished => {
//                         self.finished = true;
//                         self._chunk_buffer.clear();
//                         Ok(PyBytes::new(py, &[]))
//                     }
//                     ReaderErr::IoError(ioe) => Err(PyErr::from(ioe)),
//                     ReaderErr::Other(s) => Err(PyErr::new::<PyValueError, _>(s)),
//                 },
//             }
//         }
//     }
// }

// // Not sure when this unwrap will fail
// static RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());
