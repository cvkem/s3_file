use std::{
    cmp,
    io,
    mem,
    sync::{Arc, Mutex}
};
use bytes::{BytesMut, BufMut};
use futures::executor::block_on;

use crate::object_writer::{ObjectWriter, ObjectWriterAux};


pub struct S3Writer {
    block_size: usize,
    started: bool,
    buffer: BytesMut,
    source: Arc<Mutex<ObjectWriter>>,
    num_blocks: usize,
    length: usize
}


impl S3Writer {
    
    /// create a new S3File with an LRU-cache to support fast (sequential) read operations
    pub fn new(bucket: String, object: String, block_size: usize) -> Self {
        let source = Arc::new(Mutex::new(ObjectWriter::new(bucket, object)));

        let buffer = BytesMut::with_capacity(block_size);
    
        Self{
            block_size,
            started: false,
            buffer,
            source,
            num_blocks: 0,
            length: 0
        }
    }

    // flush the current buffer (Auxiliary as we also need it before the io::Write implementation is known.)
    fn flush_aux(&mut self) -> io::Result<()> {

        if !self.started {
            // do initialization at the last possible moment, such that we reduce risk on debris.
            block_on(self.source.lock().unwrap().create_multipart_upload())?;
            self.started = true;           
        }

        if self.buffer.len() < 1 {
            println!("No bytes to flush");
            return Ok(());
        }
        let mut buffer = BytesMut::with_capacity(self.block_size);
        mem::swap(&mut self.buffer, &mut buffer);

        let buffer = buffer.freeze(); // automatically truncates to the used length!
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

}


impl io::Write for S3Writer {

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut num_written = 0_usize;
        let mut start_pos = 0_usize;

        while start_pos < buf.len() {
            // we should create the buffer here, such that it is allocated last-minute if it does not exist.
            let buff_space = self.block_size - self.buffer.len();
            let max_to_write = buf.len() - start_pos;
            let to_write = cmp::min(buff_space, max_to_write);
            let end_pos = start_pos + to_write;

            let bytes_to_write = &buf[start_pos..end_pos];

            self.buffer.put(bytes_to_write);
            
            if self.buffer.len() >= self.block_size {
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
