use std::io::{Read, Result as IOResult, Seek, SeekFrom, Error as IOError, ErrorKind as IOErrorKind};
// use std::ptr;
use std::str;
use std::cmp;


use crate::lru_cache::LruCache;
use crate::source::ObjectSource;



pub struct S3File<'a> {
    cache: LruCache<'a>,
    source: ObjectSource,
    position: usize
}


impl S3File<'_> {
    
    /// create a new S3File with an LRU-cache to support fast (sequential) read operations
    pub fn new(bucket: String, object: String, block_size: usize) -> Self {
        let source = ObjectSource::new(bucket, object);
        let cache = LruCache::new(10, block_size, &source); 

        Self{
            cache,
            source,
            position: 0
        }
    }

    /// get the filled cache-block and fill up the buffer over to at most 'max_len' bytes. Return the number of read bytes.
    fn read_segment(&mut self, buffer: &mut[u8], max_len: usize) -> usize {
        let block_idx = self.cache.find_cached_block(self.position);
        let block = &self.cache.cache[block_idx];
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

}


impl Read for S3File<'_> {
    fn read(&mut self, buff: &mut [u8]) -> IOResult<usize> {
        let buff_len = buff.len();
        let mut read_len = 0;
        let mut window: &mut  [u8] = buff;
        while buff_len - read_len > 0 {
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

impl Seek for S3File<'_> {
    fn seek(&mut self, pos: SeekFrom) -> IOResult<u64> {
        let new_pos: i64 = match pos {
            SeekFrom::Start(upos) =>  upos as i64,
            SeekFrom::Current(ipos) =>   self.position as i64 + ipos,
            // SeekFrom::End(ipos) -> {
            //     match self.get_length() {
            //         Ok(len) -> len as i64 + ipos,
            //         Err(e) -> return Err(e)
            //     }
            SeekFrom::End(ipos) => self.source.get_length()? as i64 + ipos
            }; 

        // check the validity of the new position
        if  new_pos < 0 {
            return Err(IOError::new(IOErrorKind::InvalidInput, "Position should not before 0."));
        } else if new_pos > self.source.get_length()? as i64 {
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
