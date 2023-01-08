use std::{
    sync::{Arc, Mutex},
    time::Instant};
use bytes::Bytes;
use futures::executor::block_on;


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
    pub cache: Vec<ObjBlock>  // should be private, but then find_cache_block should return a reference. TODO: fix this
}


impl LruCache {

    pub fn new(num_blocks: usize, block_size: usize, source: Arc<Mutex<ObjectReader>>) -> Self {
        LruCache {block_size, 
            source,
            cache: Vec::<ObjBlock>::with_capacity(num_blocks)}
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

    /// get a block from object-storage that contains byte-position 'start' and append it to the cache.
    /// return the index of the block.
    fn get_block_from_store(&mut self, start: usize) -> usize {
        let block_start = (start / self.block_size) * self.block_size;
        //let end_block = cmp::min(block_start + self.block_size, self.get_length());
        let block_end = block_start + self.block_size - 1;  // end is inclusive

        // create the block and fill it with data
        let f = async {

            let data = self.source.lock().unwrap().get_bytes(block_start, block_end).await;
            
            let new_block = ObjBlock {
                start: block_start,
                last_used: Instant::now(),
                data
            };
            self.cache.push(new_block);
        };
        block_on(f);
        self.cache.len() - 1
    }

     /// find the block in cache that contains byte-position 'start' of the full object and read from s3 if needed. Returns the index of the block in the 'cache'.
     pub fn find_cached_block(&mut self, start: usize) -> usize {
        for (idx, ob) in self.cache.iter().enumerate() {
            if ob.start <= start && start < ob.start + ob.data.len() as usize {
                return idx
            } 
        }
        // block is not loaded yet
        if self.cache.len() >= self.cache.capacity() {
            self.free_lru();
        };

        self.get_block_from_store(start)
    }


}