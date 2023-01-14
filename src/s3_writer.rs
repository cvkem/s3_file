use std::{
    cmp,
    io,
    sync::{Arc, Mutex}, 
//    sync::mpsc,
//    thread
};
use bytes::{BytesMut, BufMut};
use crate::write_sink::WriteSink;
use crate::object_writer::{ObjectWriter, MIN_CHUNK_SIZE};



enum S3WriterState {
    None,
    Multipart(Arc<Mutex<WriteSink>>),
    Done   // either a single file uploaded, of ObjectWriter has been closed.
}


impl S3WriterState {
    fn is_none(&self) -> bool {
        if let S3WriterState::None = self {true } else {false}
    }

    fn is_done(&self) -> bool {
        if let S3WriterState::Done = self {true } else {false}
    }

    fn is_multipart(&self) -> bool {
        if let S3WriterState::Multipart(_) = self {true } else {false}
    }

    // get writer or return an error.
    fn get_write_sink(&self) -> io::Result<Arc<Mutex<WriteSink>>> {
        if let S3WriterState::Multipart(write_sink) = self {
            Ok(write_sink.clone())
        } else {
            match self {
                S3WriterState::None => Err(io::Error::new(io::ErrorKind::Other, "Failed to get writer. Multi-part writer not initialized.")),
                S3WriterState::Done => Err(io::Error::new(io::ErrorKind::Other, "Failed to get writer. Writer was closed (or single-part file was written).")),
                _ => panic!("Unknown state of S3WriterState")
            }            
        }
    }

}

pub struct S3Writer {
    bucket_name: String,
    object_name: String,
    block_size: usize,
    state: S3WriterState,
    buffer: Option<BytesMut>,
    num_blocks: usize,
    length: usize
}


impl S3Writer {
    
    /// create a new S3File with an LRU-cache to support fast (sequential) read operations
    pub fn new(bucket_name: String, object_name: String, block_size: usize) -> Self {
        if block_size < MIN_CHUNK_SIZE {
            println!("The minimal block-size for a multi-part upload is 5Mb!");
        }

        Self{
            bucket_name,
            object_name,
            block_size,
            state: S3WriterState::None,
            buffer: None,
            num_blocks: 0,
            length: 0
        }
    }

    // flush the current buffer (Auxiliary as we also need it before the io::Write implementation is known.)
    fn flush_aux(&mut self) -> io::Result<()> {
        assert!(!self.state.is_done(), "S3Writer has state Done so we can not write or flush to it.");

        let buffer = match self.buffer.take() {
            None => { 
                    println!("Buffers is empty, so nothing to flush.");
                    return Ok(())
                }
            Some (buffer) => buffer.freeze()
        };

        if self.state.is_none() {
            if buffer.len() < MIN_CHUNK_SIZE {
                // we should the buffer in one pass as small batches (<5Mb) are not possible in an S3 multipart upload.
                println!("Buffer has length {} while the block_size is {}, thus using write single-blob method", buffer.len(), self.block_size);
                ObjectWriter::single_shot_upload(&self.bucket_name, &self.object_name, buffer)?;
                // we are ready so return
                self.state = S3WriterState::Done;
                return Ok(());
            } else {                
                let write_sink = WriteSink::new(self.bucket_name.to_owned(), self.object_name.to_owned());
                self.state = S3WriterState::Multipart(Arc::new(Mutex::new(write_sink)));
            }
        }

        self.num_blocks += 1;
        self.length += buffer.len();

        self.state.get_write_sink()?.lock().unwrap().send_bytes(buffer);
        
        Ok(())
    }


    /// flush all data and close the S3Writer.
    pub fn close(mut self) -> io::Result<()> {
        if self.state.is_multipart() {
            self.flush_aux()?
        }

        match &self.state {
            S3WriterState::None if self.buffer == None => Ok(()),
            S3WriterState::None  =>  {
                    // we can write the buffer in one pass. Some duplication of code in Flush_aux.
                    // however, using flush_aux fails when this is the first buffer and exactly contains block_size
                    // the alternative is to increase block-size by 1, which is ugly, but would work too.
                    let buffer = self.buffer.take().unwrap().freeze(); 
                    ObjectWriter::single_shot_upload(&self.bucket_name, &self.object_name, buffer)?;
                    // we are ready so return
                    self.state = S3WriterState::Done;
                    Ok(())
                },
            S3WriterState::Done => Ok(()), 
            S3WriterState::Multipart(write_sink) => write_sink.lock().unwrap().close()
        }
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
