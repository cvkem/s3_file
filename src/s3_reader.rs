use std::{
    cmp,
    io::{
        Read, 
        Result as IOResult, 
        Seek, SeekFrom, 
        Error as IOError, 
        ErrorKind as IOErrorKind},
    str,
    sync::{Arc, Mutex}};


use crate::lru_cache::LruCache;
use crate::object_reader::ObjectReader;



pub struct S3Reader {
    cache: Arc<Mutex<LruCache>>,
    source: Arc<Mutex<ObjectReader>>,
    pub position: usize
}


impl S3Reader {
    
    /// create a new S3File with an LRU-cache to support fast (sequential) read operations
    pub fn new(bucket: String, object: String, block_size: usize) -> Self {
        let source = Arc::new(Mutex::new(ObjectReader::new(bucket, object)));
        let cache = LruCache::new(10, block_size, Arc::clone(&source)); 
        let cache = Arc::new(Mutex::new(cache));

        Self{
            cache,
            source,
            position: 0
        }
    }

    /// get the filled cache-block and fill up the buffer over to at most 'max_len' bytes. Return the number of read bytes.
    fn read_segment(&mut self, buffer: &mut[u8], max_len: usize) -> usize {
        let block = self.cache.lock().unwrap().find_cached_block(self.position);
//        let block = &self.cache.lock().unwrap().cache[block_idx];
        let relative_position = self.position - block.start;
        let read_len = cmp::min(max_len, block.data.len() - relative_position);

        let src_slice = block.data.slice(relative_position..relative_position+read_len);
        let dst_slice = &mut buffer[0..read_len];
        dst_slice.copy_from_slice(&src_slice);
        self.position += read_len;
        // copy data
        // let src_ptr = block.data.slice(relative_position..relative_position+read_len).as_ptr();
        // let dst_ptr = buffer.as_mut_ptr();
        // unsafe { ptr::copy_nonoverlapping(src_ptr, dst_ptr, read_len) };
        // see example in: https://doc.rust-lang.org/std/ptr/fn.copy_nonoverlapping.html why next line is adviced
        //dst_ptr.set_len(read_len);

        read_len
    }

    fn get_length(&self) -> IOResult<u64> {
        Ok(self.source.lock().unwrap().get_length()?)
    }

}


impl Read for S3Reader {
    fn read(&mut self, buff: &mut [u8]) -> IOResult<usize> {
        let buff_len = buff.len();
        let mut read_len = 0;
        let mut window: &mut  [u8] = buff;
        let object_length = self.get_length()?;
        while (buff_len - read_len > 0) & ((self.position as u64) < object_length) {
            println!("Read segment after {} bytes to Window for at most {} bytes.", read_len, buff_len - read_len);
            let len = self.read_segment(window, buff_len - read_len);
            //shift the window forward (position has been updated already)
            window = &mut window[len..];
            read_len += len;
        }
        println!("Read buff '{}'.", str::from_utf8(&buff).unwrap());
        Ok(read_len)
    }
}

impl Seek for S3Reader {
    fn seek(&mut self, pos: SeekFrom) -> IOResult<u64> {
        let new_pos: i64 = match pos {
            SeekFrom::Start(upos) =>  upos as i64,
            SeekFrom::Current(ipos) =>   self.position as i64 + ipos,
            // SeekFrom::End(ipos) -> {
            //     match self.get_length() {
            //         Ok(len) -> len as i64 + ipos,
            //         Err(e) -> return Err(e)
            //     }
            SeekFrom::End(ipos) => self.source.lock().unwrap().get_length()? as i64 + ipos
            }; 

        // check the validity of the new position
        if  new_pos < 0 {
            return Err(IOError::new(IOErrorKind::InvalidInput, "Position should not before 0."));
        } else if new_pos > self.source.lock().unwrap().get_length()? as i64 {
            return Err(IOError::new(IOErrorKind::UnexpectedEof, "Position beyond size of S3-object."));
        }

        self.position = new_pos as usize;
        Ok(self.position as u64)
    }

//    fn rewind(&mut self) -> Result<()> { ... }
//    fn stream_len(&mut self) -> Result<u64> { ... }
    
    fn stream_position(&mut self) -> IOResult<u64> {
        Ok(self.position as u64)
    }
}



// Extensions needed for ChunkReader
// should be under feature flag.

use std::{
    clone::Clone,
};
use  parquet::file::reader::Length;

impl Clone for S3Reader {

    fn clone(&self) -> Self {
        let cache = Arc::clone(&self.cache);
        let source = Arc::clone(&self.source);
        let position = self.position;

        S3Reader {
            cache,
            source,
            position
        }
    }

    fn clone_from(&mut self, source: &Self) { 
        self.cache = Arc::clone(&source.cache);
        self.source = Arc::clone(&source.source);
        self.position = source.position;
    }

}

impl Length for S3Reader {

    fn len(&self) -> u64 {
        self.get_length().unwrap()
    }
}

