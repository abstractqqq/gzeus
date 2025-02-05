use memchr::{memmem, memmem::FinderRev};
use std::io::{Error, Read};
// use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Debug)]
pub enum ReaderErr {
    Finished,
    IoError(Error),
    Other(String),
}

pub struct CsvChunker {
    leftover_chunk: Vec<u8>,
    pub(crate) finder: FinderRev<'static>,
}

impl CsvChunker {
    pub fn new(line_change_symbol: &str) -> Self {
        Self {
            leftover_chunk: vec![],
            finder: memmem::FinderRev::new(line_change_symbol).into_owned(),
        }
    }

    /// Pushes the leftover to the write_buffer and splits the write_buffer into 2 (left and right)
    /// Left is the part with the leftover. Right is the part that should be written by the reader.
    /// After that, clears the leftover, and returns (size of leftover pushed, the right buffer)
    #[inline]
    pub fn push_leftover_to_buffer<'a>(&mut self, buffer: &'a mut [u8]) -> (usize, &'a mut [u8]) {
        let (left, right) = buffer.split_at_mut(self.leftover_chunk.len());
        left.copy_from_slice(&self.leftover_chunk);
        self.leftover_chunk.clear();
        (left.len(), right)
    }

    /// Process the read result. If reader succeeds and returns an int n,
    /// We find the last position of a line change symbol in the range 0..n by using memchr to search
    /// the bytes. The last index will be = last position of the line change + 1. The last_index..n
    /// part will be the new leftover. Finally, return the number of bytes in the buffer we actually
    /// populated. (0..last_index for most of the cases.)
    #[inline]
    pub fn process_read_result(
        &mut self,
        result: Result<usize, std::io::Error>,
        buffer: &[u8],
    ) -> Result<usize, ReaderErr> {
        match result {
            Ok(n) => {
                if n == 0 {
                    Err(ReaderErr::Finished)
                } else {
                    match self.finder.rfind(&buffer[0..n]) {
                        Some(j) => {
                            let last_index = j + 1;
                            // Leftover is cleaned in `push_leftover_to_buffer`. So we can extend from slice.
                            self.leftover_chunk
                                .extend_from_slice(&buffer[last_index..n]);
                            Ok(last_index)
                        }
                        None => {
                            // No more separtor. This means we have reached the end and the end
                            // doesn't have a separator.
                            // Data is read into the right buffer.
                            self.leftover_chunk.shrink_to_fit();
                            Ok(n)
                        }
                    }
                }
            }
            Err(e) => Err(ReaderErr::IoError(e)),
        }
    }

    pub fn read_and_write<R: Read>(
        &mut self,
        reader: &mut R,
        write_buffer: &mut [u8],
    ) -> Result<usize, ReaderErr> {
        let (leftover_size, clean_buffer) = self.push_leftover_to_buffer(write_buffer);
        let read_result = reader.read(clean_buffer);
        self.process_read_result(read_result, clean_buffer)
            .map(|n| n + leftover_size) // n + leftover_size = actual valid index range: 0..this value
    }

    // // Will be executed in a tokio runtime and will block
    // pub async fn async_read_and_write<R: AsyncRead + std::marker::Unpin>(
    //     &mut self,
    //     reader: &mut R,
    //     write_buffer: &mut [u8],
    // ) -> Result<usize, ReaderErr> {
    //     let (leftover_size, clean_buffer) = self.push_leftover_to_buffer(write_buffer);
    //     let read_result = reader.read(clean_buffer).await;
    //     self.process_read_result(read_result, clean_buffer)
    //         .map(|n| n + leftover_size)
    // }
}
