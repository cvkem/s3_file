#![feature(async_fn_in_trait)]
use std::{
    clone::Clone,
    cmp,
    io::{
        Read, 
        Result as IOResult, 
        Seek, SeekFrom, 
        Error as IOError, 
        ErrorKind as IOErrorKind},
    sync::Arc};
use tokio::sync::Mutex;
use  parquet::file::reader::Length;


use crate::{lru_cache::LruCache, async_bridge};
use crate::object_reader::ObjectReader;

pub struct S3Reader {
    cache: Arc<Mutex<LruCache>>,
    source: Arc<Mutex<ObjectReader>>,
    position: Arc<Mutex<usize>>   // Mutex needed such that static_clones write to the same value. Normal Clone allows positions to diverge 
}


impl S3Reader {
    
    /// create a new S3File with an LRU-cache to support fast (sequential) read operations
    pub fn new(bucket: String, object: String, block_size: usize) -> Self {
        let source = Arc::new(Mutex::new(ObjectReader::new(bucket, object)));
        let cache = LruCache::new(10, block_size, Arc::clone(&source)); 
        let cache = Arc::new(Mutex::new(cache));
        let position = Arc::new(Mutex::new(0 as usize));

        Self{
            cache,
            source,
            position
        }
    }

    /// get the filled cache-block and fill up the buffer over to at most 'max_len' bytes. Return the number of read bytes.
    async fn read_segment(&mut self, buffer: &mut[u8], max_len: usize) -> usize {
        let position = self.get_position().await;
        let block = self.cache.lock().await.find_cached_block(position);
        let relative_position = position - block.start;
        let read_len = cmp::min(max_len, block.data.len() - relative_position);

        let src_slice = block.data.slice(relative_position..relative_position+read_len);
        let dst_slice = &mut buffer[0..read_len];
        dst_slice.copy_from_slice(&src_slice);
        self.update_position_s(read_len.try_into().unwrap()).unwrap();
        // copy data
        // let src_ptr = block.data.slice(relative_position..relative_position+read_len).as_ptr();
        // let dst_ptr = buffer.as_mut_ptr();
        // unsafe { ptr::copy_nonoverlapping(src_ptr, dst_ptr, read_len) };
        // see example in: https://doc.rust-lang.org/std/ptr/fn.copy_nonoverlapping.html why next line is adviced
        //dst_ptr.set_len(read_len);

        read_len
    }


    /// Get the length of the s3-object
    pub async fn get_length(&self) -> IOResult<u64> {
        Ok(self.source.lock().await.get_length()?)
    }

    /// Get the current position in the S3 object
    pub async fn get_position(&self) -> usize {
        let pos = self.position.lock().await;
        *pos
    }

    /// Set the current position in the S3 object. 
    /// The input is an i64 such that the bound checks can be performed in this function (in case of computed relative positions)
    pub async fn set_position(&self, new_pos: i64) -> IOResult<()> {
        // check whether the new_pos is a valid position for the current S3-object
        if  new_pos < 0 {
            return Err(IOError::new(IOErrorKind::InvalidInput, "Position should not before 0."));
        } else if new_pos > self.get_length().await? as i64 {
            return Err(IOError::new(IOErrorKind::UnexpectedEof, "Position beyond size of S3-object."));
        }

        let mut pos = self.position.lock().await;
        let new_pos: usize = new_pos.try_into().unwrap();
        *pos = new_pos;

        Ok(())
    }

    /// Set the update the position in the S3 object via a delta (a relative change). 
    /// The input is an i64 such that the bound checks can be performed in this function (in case of computed relative positions)
    pub async fn update_position(&self, delta: i64) -> IOResult<()> {
        // check whether the new_pos is a valid position for the current S3-object
        let mut pos = self.position.lock().await;
        let new_pos = *pos as i64 + delta;

        if  new_pos < 0 {
            return Err(IOError::new(IOErrorKind::InvalidInput, "Position should not before 0."));
        } else if new_pos > self.get_length().await? as i64 {
            return Err(IOError::new(IOErrorKind::UnexpectedEof, "Position beyond size of S3-object."));
        }

        let new_pos: usize = new_pos.try_into().unwrap();
        *pos = new_pos;

        Ok(())
    }



    /// Clone used when a static future is needed.
    fn static_clone(&self) -> Self {
        let cache = Arc::clone(&self.cache);
        let source = Arc::clone(&self.source);
        let position = Arc::clone(&self.position);

        S3Reader { cache, source, position}
    }

    /// Synchronous (blocking) version of read_segment
    pub fn read_segment_s(&mut self, buffer: &mut[u8], max_len: usize) -> usize {
        let mut self_clone = self.static_clone();
        async_bridge::run_async(async move { self_clone.read_segment(buffer, max_len).await })
    }

    /// Synchronous (blocking) version of get_length()
    pub fn get_length_s(&self) -> IOResult<u64> {
        let self_clone = self.static_clone();
        async_bridge::run_async(async move { self_clone.get_length().await })
    }

    /// Synchronous (blocking) version of get_position()
    pub fn get_position_s(&self) -> usize {
        let self_clone = self.static_clone();
        async_bridge::run_async(async move { self_clone.get_position().await })
    }

    /// Synchronous (blocking) version of set_position()
    pub fn set_position_s(&self, new_pos: i64) -> IOResult<()> {
        let self_clone = self.static_clone();
        async_bridge::run_async(async move { self_clone.set_position(new_pos).await })
    }

    /// Synchronous (blocking) version of set_position()
    pub fn update_position_s(&self, delta: i64) -> IOResult<()> {
        let self_clone = self.static_clone();
        async_bridge::run_async(async move { self_clone.update_position(delta).await })
    }

}


impl Read for S3Reader {
    fn read(&mut self, buff: &mut [u8]) -> IOResult<usize> {
        let buff_len = buff.len();
        let mut read_len = 0;
        let mut window: &mut  [u8] = buff;
        let object_length = self.get_length_s()?;
        while (buff_len - read_len > 0) & ((self.get_position_s() as u64) < object_length) {
//            println!("Read segment after {} bytes to Window for at most {} bytes.", read_len, buff_len - read_len);
            let len = self.read_segment_s(window, buff_len - read_len);
            //shift the window forward (position has been updated already)
            window = &mut window[len..];
            read_len += len;
        }
//        println!("Read buff '{}'.", str::from_utf8(&buff).unwrap());
        Ok(read_len)
    }
}

impl Seek for S3Reader {
    fn seek(&mut self, pos: SeekFrom) -> IOResult<u64> {
        let new_pos: i64 = match pos {
            SeekFrom::Start(upos) =>  upos as i64,
            SeekFrom::Current(ipos) =>   self.get_position_s() as i64 + ipos,
            // SeekFrom::End(ipos) -> {
            //     match self.get_length() {
            //         Ok(len) -> len as i64 + ipos,
            //         Err(e) -> return Err(e)
            //     }
            SeekFrom::End(ipos) => self.get_length_s()? as i64 + ipos
            }; 

        // check the validity of the new position
        if  new_pos < 0 {
            return Err(IOError::new(IOErrorKind::InvalidInput, "Position should not before 0."));
        } else if new_pos > self.get_length_s()? as i64 {
            return Err(IOError::new(IOErrorKind::UnexpectedEof, "Position beyond size of S3-object."));
        }

        self.set_position_s(new_pos);
        Ok(new_pos as u64)
    }

//    fn rewind(&mut self) -> Result<()> { ... }
//    fn stream_len(&mut self) -> Result<u64> { ... }
    
    fn stream_position(&mut self) -> IOResult<u64> {
        Ok(self.get_position_s() as u64)
    }
}



// Extensions needed for ChunkReader
// should be under feature flag.


impl Clone for S3Reader {

    fn clone(&self) -> Self {
        let cache = Arc::clone(&self.cache);
        let source = Arc::clone(&self.source);
        // For the position we create a new mutex such that the clone can change position independent of its parent.
        let position = Arc::new(Mutex::new(self.get_position_s()));

        S3Reader {
            cache,
            source,
            position
        }
    }

    fn clone_from(&mut self, source: &Self) { 
        self.cache = Arc::clone(&source.cache);
        self.source = Arc::clone(&source.source);
        // For the position we create a new mutex such that the clone can change position independent of its parent.
        self.position = Arc::new(Mutex::new(source.get_position_s()));
    }

}


impl Length for S3Reader {

    fn len(&self) -> u64 {
        self.get_length_s().unwrap()
    }
}

