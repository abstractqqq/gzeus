use memchr::{memmem, memmem::FinderRev};
use std::io::{Error, Read};

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
    pub fn new(new_line_symbol: &str) -> Self {
        Self {
            leftover_chunk: vec![],
            finder: memmem::FinderRev::new(new_line_symbol).into_owned(),
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

    pub fn read_and_write<R: Read>(
        &mut self,
        reader: &mut R,
        write_buffer: &mut [u8],
    ) -> Result<usize, ReaderErr> {
        let (leftover_size, clean_buffer) = self.push_leftover_to_buffer(write_buffer);
        let capacity = clean_buffer.len();
        let mut cursor: usize = 0;
        while cursor < capacity {
            match reader.read(&mut clean_buffer[cursor..]) {
                Ok(n) => {
                    if n <= 0 {
                        return Err(ReaderErr::Finished);
                    } else {
                        cursor += n;
                    }
                }
                Err(e) => return Err(ReaderErr::IoError(e)),
            }
        }
        // Find index of last line change symbol
        let new_bytes = match self.finder.rfind(&clean_buffer[..cursor]) {
            Some(j) => {
                let last_index = j + 1;
                // Leftover is cleaned in `push_leftover_to_buffer`. So we can extend from slice.
                self.leftover_chunk
                    .extend_from_slice(&clean_buffer[last_index..]);
                last_index
            }
            // Reached the end. No more line change
            None => cursor,
        };

        Ok(leftover_size + new_bytes)
    }
}
