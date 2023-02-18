
use std::{
    cell::RefCell,
    cmp,
    io::{self, Read}};
use parquet::{
    errors::Result,
    file::reader::{ChunkReader}};
use crate::s3_reader::S3Reader;


impl ChunkReader for S3Reader {
    type T = S3ReaderChunk;

    fn get_read(&self, start: u64, length: usize) -> Result<Self::T> {
        Ok(S3ReaderChunk::new(self, start, length))
    }
}


pub struct S3ReaderChunk {
    reader: RefCell<S3Reader>,
    start: u64,
    end: u64
}

impl S3ReaderChunk {
    pub fn new(s3_reader: &S3Reader, start: u64, length: usize) -> Self {
        let mut reader = s3_reader.clone();
        reader.set_position_s(start.try_into().unwrap());
        let reader = RefCell::new(reader);
        Self {
            reader,
            start,
            end: start + length as u64,
        }
    }
}


impl Read for S3ReaderChunk {

    /// sequential read of the chunk, ut do not read beyond the end of the chunk!
    fn read(&mut self, buff: &mut [u8]) -> io::Result<usize> {
        let len = buff.len();
        let pos = {self.reader.borrow().get_position_s()};
        let max_len = self.end - (pos as u64);
        let max_buff_len: usize = cmp::min(len, max_len.try_into().unwrap());
        let max_buff = &mut buff[..max_buff_len];

        self.reader.borrow_mut().read(max_buff)
    }
}

// impl ChunkReader for S3Reader {
//     type T = S3ReaderChunk<S3Reader>;

//     fn get_read(&self, start: u64, length: usize) -> Result<Self::T> {
//         Ok(S3ReaderChunk::new(self, start, length))
//     }
// }


// struct S3ReaderChunk<R :Read> {
//     reader: RefCell<R>,
//     start: u64,
//     end: u64
// }

// impl<R: Read> S3ReaderChunk<R> {
//     pub fn new(fd: &R, start: u64, length: usize) -> Self {
//         let s3_reader: &S3Reader = fd;
//         let reader = S3Reader::clone(s3_reader);
//         let reader = RefCell::new(reader);
// //        let reader = RefCell::new(fd);
//         Self {
//             reader,
//             start,
//             end: start + length as u64,
//             // buf: vec![0_u8; DEFAULT_BUF_SIZE],
//             // buf_pos: 0,
//             // buf_cap: 0,
//         }
//     }
// }


// impl<R: Read> Read for S3ReaderChunk<R> {
//     fn read(&mut self, buff: &mut [u8]) -> io::Result<usize> {
//         self.reader.borrow_mut().read(buff)
//     }
// }
