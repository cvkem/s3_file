
use std::{
    cell::RefCell,
    io::{self, Read},
    sync::Arc};
use parquet::{
    file::reader::{ChunkReader, Length},
    io::util::TryClone};
use crate::s3_reader::S3Reader;


impl ChunkReader for S3Reader {
    type T = S3ReaderChunk<S3Reader>;

    fn get_read(&self, start: u64, length: usize) -> io::Result<Self::T> {
        Ok(S3ReaderChunk::new(self, start, length))
    }
}

impl TryClone for S3Reader {
    fn try_clone(&self) -> Result<Self> {
        let cache = Arc::clone(&self.cache);
        let source = Arc::clone(&self.source);
        let position = self.position;

        let clone = S3Reader {
            cache,
            source,
            position
        };
        Ok(clone)
    }
}

struct S3ReaderChunk<R :Read> {
    reader: RefCell<R>,
    start: u64,
    end: u64
}

impl<R: Read> S3ReaderChunk<R> {
    pub fn new(fd: &R, start: u64, length: usize) -> Self {
        let reader = RefCell::new(fd.try_clone().unwrap());
//        let reader = RefCell::new(fd);
        Self {
            reader,
            start,
            end: start + length as u64,
            // buf: vec![0_u8; DEFAULT_BUF_SIZE],
            // buf_pos: 0,
            // buf_cap: 0,
        }
    }
}


impl<R: Read> Read for S3ReaderChunk<R> {

}

impl Length for S3Reader {

}
