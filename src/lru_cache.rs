use std::{
    fmt,
    sync::Arc,
    time::Instant};
use tokio::sync::Mutex;
use bytes::Bytes;
use async_bridge;


use crate::object_reader::{
    GetBytes,
    ObjectReader};


pub struct ObjBlock {
    pub start: usize,
    last_used: Instant,
    pub data: Bytes
}

pub struct LruCache {
    block_size: usize,
    source: Arc<Mutex<ObjectReader>>,
    cache: Vec<Arc<ObjBlock>>  // should be private, but then find_cache_block should return a reference. TODO: fix this
}

impl fmt::Debug for LruCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LruCache")
         .field("block_size", &self.block_size)
//         .field("source-object-name", &self.source.get_object_name())
         .field("cache-length", &self.cache.len())
         .finish()
    }
}

impl LruCache {

    pub fn new(num_blocks: usize, block_size: usize, source: Arc<Mutex<ObjectReader>>) -> Self {
        LruCache {block_size, 
            source,
            cache: Vec::<Arc<ObjBlock>>::with_capacity(num_blocks)}
    }

    /// free the Least Recent Used page to make more room in the cache
    fn free_lru(&mut self) {
        if self.cache.len() < 1 {
            panic!("No block to free");
        }
        let mut oldest = Instant::now();
        let mut oldest_idx = 0_usize;

        for (idx, ob) in self.cache.iter().enumerate() {
            if ob.last_used < oldest {
                oldest = ob.last_used.clone();
                oldest_idx = idx;
            }
        }
        self.cache.remove(oldest_idx);
    }

    /// get an Arc-clone (reference counted) of block Idx of the cache.
    fn get_block_arc(&self, idx: usize) -> Arc<ObjBlock> {
        Arc::clone(&self.cache[idx])
    }

    //TODO: update function such that it returns an Arc<&ObjBlock> such that the code is safe in a multi-threaded context!
    /// get a block from object-storage that contains byte-position 'start' and append it to the cache.
    /// return the index of the block.
    fn get_block_from_store(&mut self, start: usize) -> Arc<ObjBlock> {
        let block_start = (start / self.block_size) * self.block_size;
        //let end_block = cmp::min(block_start + self.block_size, self.get_length());
        let block_end = block_start + self.block_size - 1;  // end is inclusive

        let source = self.source.clone();
        // create the block and fill it with data
        let f = async {
//        let f = async move {
                let obj_rdr = source.lock().await;
            let get_bytes_fut = obj_rdr.get_bytes(block_start, block_end);
            let data = get_bytes_fut.await;
            
            let new_block = ObjBlock {
                start: block_start,
                last_used: Instant::now(),
                data
            };
            self.cache.push(Arc::new(new_block));
        };
        // block_on(f);
        async_bridge::run_async(f);
        
        self.get_block_arc(self.cache.len() - 1)
    }

    //TODO: update function such that it returns an Arc<&ObjBlock> such that the code is safe in a multi-threaded context!
     /// find the block in cache that contains byte-position 'start' of the full object and read from s3 if needed. Returns the index of the block in the 'cache'.
     pub fn find_cached_block(&mut self, start: usize) -> Arc<ObjBlock> {
        for (idx, ob) in self.cache.iter().enumerate() {
            if ob.start <= start && start < ob.start + ob.data.len() as usize {
                return self.get_block_arc(idx)
            } 
        }
        // block is not loaded yet
        if self.cache.len() >= self.cache.capacity() {
            self.free_lru();
        };

        self.get_block_from_store(start)
    }


}