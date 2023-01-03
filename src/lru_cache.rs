
use std::time::Instant;
use std::any::type_name;
use std::ptr;
use std::str;
use std::cmp;
use bytes::Bytes;

use crate::source::GetBytes;


struct ObjBlock {
    start: usize,
    last_used: Instant,
    data: Bytes
}


pub struct LRU_cache<'a> {
    block_size: usize,
    source: &'a impl GetBytes;
    cache: Vec<ObjBlock>
}


impl LRU_cache {
    fn new<'a>(num_blocks: usize, block_size: usize, source: &<'a> impl GetBytes) ->Self {
        let cache = Vec::with_capacity(num_blocks);
        LRU_cache {block_size, 
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
     // append a block to the cache that contains the byte start and append it to the cache
     fn get_block_from_store(&mut self, start: usize) -> usize {
        let block_start = (start / self.block_size) * self.block_size;
        //let end_block = cmp::min(block_start + self.block_size, self.get_length());
        let block_end = block_start + self.block_size - 1;  // end is inclusive

        // create the block and fill it with data
        println!("\nAbout to fetch object {}::{} for range='{}'", &self.bucket, &self.object, &range);
        let f = async {

            let data = source.get_bytes(block_start, block_end);
            
            let mut new_block = ObjBlock {
                start: block_start,
                last_used: Instant::now(),
                data
            };
            self.cache.push(new_block);
            println!("Pushed the block to the cache. Current length={}", self.cache.len());
        };
        block_on(f);
        self.cache.len() - 1
    }

       /// find the block in cache that contains 'start' and read from s3 if needed. Returns the index of the block in the 'cache'.
       fn find_cached_block(&mut self, start: usize) -> usize {
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