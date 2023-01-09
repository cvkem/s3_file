use std::{
    cmp,
    io,
    sync::{Arc, Mutex}
};
use bytes::{BytesMut, BufMut};
use futures::executor::block_on;

use crate::object_writer::{ObjectWriter, ObjectWriterAux};


pub struct S3Writer {
    block_size: usize,
    started: bool,
    buffer: Option<BytesMut>,
    source: Arc<Mutex<ObjectWriter>>,
    num_blocks: usize,
    length: usize
}


impl S3Writer {
    
    /// create a new S3File with an LRU-cache to support fast (sequential) read operations
    pub fn new(bucket: String, object: String, block_size: usize) -> Self {
        let source = Arc::new(Mutex::new(ObjectWriter::new(bucket, object)));

//        let buffer = BytesMut::with_capacity(block_size);
    
        Self{
            block_size,
            started: false,
            buffer: None,
            source,
            num_blocks: 0,
            length: 0
        }
    }

    // flush the current buffer (Auxiliary as we also need it before the io::Write implementation is known.)
    fn flush_aux(&mut self) -> io::Result<()> {
        let buffer = match self.buffer.take() {
            None => { 
                    println!("Buffers is empty, so nothing to flush.");
                    return Ok(())
                }
            Some (buffer) => buffer.freeze()
        };

        if !self.started {
            // do initialization at the last possible moment, such that we reduce risk on debris.
            block_on(self.source.lock().unwrap().create_multipart_upload())?;
            self.started = true;           
        }

        self.num_blocks += 1;
        self.length += buffer.len();

        block_on(self.source.lock().unwrap().upload_part(buffer))?;
        
        Ok(())
    }

    /// flush all data and close the S3Writer.
    pub fn close(mut self) -> io::Result<()> {
        self.flush_aux()?;
        block_on(self.source.lock().unwrap().close())?;
        Ok(())
    }

    pub fn get_length(&self) -> usize {
        self.length
    }

    /// internal function to create a buffer when it does not exist yet
    /// Ensures last minute generation of a buffer, for example to prevent that the last flush
    /// tries to allocate a buffer that is never used
    fn check_get_buffer(&mut self) {
        match self.buffer {
            None => self.buffer = Some(BytesMut::with_capacity(self.block_size)),
            Some(_) => () 
        }
    }

}


impl io::Write for S3Writer {

    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let mut num_written = 0_usize;
        let mut start_pos = 0_usize;

        while start_pos < data.len() {

            self.check_get_buffer();

            let buff_space = self.block_size - self.buffer.as_ref().unwrap().len();
            let remaining_data = data.len() - start_pos;
            let to_write = cmp::min(buff_space, remaining_data);
            let end_pos = start_pos + to_write;

            let bytes_to_write = &data[start_pos..end_pos];

            self.buffer.as_mut().unwrap().put(bytes_to_write);
            
            if self.buffer.as_ref().unwrap().len() >= self.block_size {
                if let Err(err) = self.flush_aux() {
                    eprintln!("Observed error during flush of block {}", self.num_blocks);
                    return Err(err);
                };
            }
            num_written += to_write;
            start_pos = end_pos;
        }

        Ok(num_written)
    }

    // flush the current buffer
    fn flush(&mut self) -> io::Result<()> {
        self.flush_aux()
    }
    
}
