use memchr::memchr_iter;
use std::io::Read;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug, PartialEq)]
pub enum ReaderErr {
    Finished,
    IoError(String),
}

pub struct CsvChunker {
    leftover_chunk: Vec<u8>,
    pub(crate) line_change_symbol: u8,
}

impl CsvChunker {
    pub fn new(line_change_symbol: u8) -> Self {
        Self {
            leftover_chunk: vec![],
            line_change_symbol: line_change_symbol,
        }
    }

    pub fn read_and_write<R: Read>(
        &mut self,
        reader: &mut R,
        write_buffer: &mut [u8],
    ) -> Result<usize, ReaderErr> {
        // Left: the leftover bytes that don't form a complete line
        // Right: to be filled with new data
        let (left, right) = write_buffer.split_at_mut(self.leftover_chunk.len());
        left.copy_from_slice(&self.leftover_chunk);

        match reader.read(right) {
            Ok(n) => {
                if n == 0 {
                    Err(ReaderErr::Finished)
                } else {
                    match memchr_iter(self.line_change_symbol, &right[0..n])
                        .rev()
                        .next()
                    {
                        Some(j) => {
                            let last_index = j + 1;
                            self.leftover_chunk.clear();
                            self.leftover_chunk.extend_from_slice(&right[last_index..n]);
                            Ok(left.len() + last_index)
                        }
                        None => {
                            // No more separtor. This means we have reached the end and the end
                            // doesn't have a separator.
                            // Data is read into the right buffer.
                            self.leftover_chunk.clear();
                            self.leftover_chunk.shrink_to_fit();
                            Ok(left.len() + n)
                        }
                    }
                }
            }
            Err(e) => Err(ReaderErr::IoError(e.to_string())),
        }
    }

    // Will be executed in a tokio runtime and will block
    pub async fn async_read_and_write<R: AsyncRead + std::marker::Unpin>(
        &mut self,
        reader: &mut R,
        write_buffer: &mut [u8],
    ) -> Result<usize, ReaderErr> {
        let (left, right) = write_buffer.split_at_mut(self.leftover_chunk.len());
        left.copy_from_slice(&self.leftover_chunk);

        match reader.read(right).await {
            Ok(n) => {
                if n == 0 {
                    Err(ReaderErr::Finished)
                } else {
                    match memchr_iter(self.line_change_symbol, &right[0..n])
                        .rev()
                        .next()
                    {
                        Some(j) => {
                            let last_index = j + 1;
                            self.leftover_chunk.clear();
                            self.leftover_chunk.extend_from_slice(&right[last_index..n]);
                            Ok(left.len() + last_index)
                        }
                        None => {
                            // No more separtor. This means we have reached the end and the end
                            // doesn't have a separator.
                            // Data is read into the right buffer.
                            self.leftover_chunk.clear();
                            self.leftover_chunk.shrink_to_fit();
                            Ok(left.len() + n)
                        }
                    }
                }
            }
            Err(e) => Err(ReaderErr::IoError(e.to_string())),
        }
    }
}
