use flate2::bufread::GzDecoder;
use memchr::memchr_iter;
use std::io::{BufRead, Read};

#[derive(Debug)]
pub enum GzCsvChunkerErr {
    Finished,
    IoError(String),
}

pub struct GzCsvChunker<R: BufRead> {
    buffer: Vec<u8>,
    gz: GzDecoder<R>,
    leftover_chunk: Vec<u8>,
    separator: u8,
}

impl<R: BufRead> GzCsvChunker<R> {
    pub fn new(separator: u8, size: usize, gz: GzDecoder<R>) -> Self {
        GzCsvChunker {
            buffer: vec![0u8; size],
            gz: gz,
            leftover_chunk: vec![],
            separator: separator,
        }
    }

    pub fn read_and_write(&mut self, write_buffer: &mut [u8]) -> Result<usize, GzCsvChunkerErr> {
        match self.gz.read(&mut self.buffer) {
            Ok(n) => {
                if n == 0 {
                    Err(GzCsvChunkerErr::Finished)
                } else {
                    match memchr_iter(self.separator, &self.buffer[0..n]).rev().next() {
                        Some(j) => {
                            let last_index = j + 1;
                            let last_position = self.leftover_chunk.len() + last_index;
                            write_buffer[0..self.leftover_chunk.len()]
                                .copy_from_slice(&self.leftover_chunk);
                            write_buffer[self.leftover_chunk.len()..last_position]
                                .copy_from_slice(&self.buffer[..last_index]);

                            self.leftover_chunk.clear();
                            self.leftover_chunk
                                .extend_from_slice(&self.buffer[last_index..n]);
                            Ok(last_position)
                        }
                        None => {
                            // No more separtor. This means we have reached the end and the end
                            // doesn't have a separator.
                            let last_position = self.leftover_chunk.len() + n;
                            write_buffer[0..self.leftover_chunk.len()]
                                .copy_from_slice(&self.leftover_chunk);
                            write_buffer[self.leftover_chunk.len()..last_position]
                                .copy_from_slice(&self.buffer[..n]);
                            self.leftover_chunk.clear();
                            self.leftover_chunk.shrink_to_fit();
                            Ok(last_position)
                        }
                    }
                }
            }
            Err(e) => Err(GzCsvChunkerErr::IoError(e.to_string())),
        }
    }
}
