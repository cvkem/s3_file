
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Error, Region};
use aws_config::meta::region::RegionProviderChain;
use std::io::{Read, Result as IOResult};
use std::time::Instant;
use std::any::type_name;
use std::ptr;
use std::cmp;
use bytes::Bytes;
use futures::executor::block_on;

pub const REGION: &str = "eu-central-1";

mod s3_service;

struct ObjBlock {
    start: usize,
    last_used: Instant,
    data: Bytes
}

pub struct s3_file {
    client: Client,
    pub bucket: String,
    pub object: String,
    pub position: usize,
    block_size: usize,
    length: Option<usize>,
    cache: Vec<ObjBlock>
}

async fn get_client() -> Client {
    let region_provider = RegionProviderChain::first_try(Region::new(REGION));
    let region = region_provider.region().await.unwrap();

    let shared_config = aws_config::from_env().region(region_provider).load().await;
    Client::new(&shared_config)
}

impl s3_file {

    
    // create a new S3_file with an LRU-cache to support fast (sequential) read operations
    pub fn new(bucket: String, object: String, block_size: usize) -> Self {
        Self{client: block_on(get_client()), 
            bucket, 
            object, 
            position: 0,
            block_size,
            length: None, 
            cache: Vec::<ObjBlock>::with_capacity(10)}
    }

    // free the Least Recent Used page to make more room in the cache
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

    // fn get_length(&mut self) -> usize {
    //     self.length.get_or_insert_with(|| s3_service::get_length())
    // }

    // append a block to the cache that contains the byte start and append it to the cache
    fn get_block_from_store(&mut self, start: usize) -> usize {
        let block_start = (start / self.block_size) * self.block_size;
        //let end_block = cmp::min(block_start + self.block_size, self.get_length());
        let block_end = block_start + self.block_size - 1;  // end is inclusive

        // create the block and fill it with data
        let range = format!("bytes={block_start}-{block_end}");
        println!("\n\n=====================\nAbout to fetch object {}::{} for range='{}'", &self.bucket, &self.object, &range);
        let get_obj_output = block_on(s3_service::download_object(&self.client, &self.bucket, &self.object, Some(range)));
        println!("Received object {:?}", get_obj_output);
        // set length when nog readily available, as we get this information free of charge here.
        _ = self.length.get_or_insert(get_obj_output.content_length() as usize);
        let agg_bytes = block_on(get_obj_output.body.collect()).expect("Failed to read data");
        // turn into bytes and take a (ref-counted) full slice out of it (reuse of same buffer)
        // Operating on AggregatedBytes directy would be more memory efficient (however, working with non-continguous memory in that case)
        let data = agg_bytes.into_bytes();
        let mut new_block = ObjBlock {
            start: block_start,
            last_used: Instant::now(),
            data
        };
        self.cache.push(new_block);
        self.cache.len() - 1
    }

    // find the block in cache that contains 'start' and read from s3 if needed. Returns the index of the block in the 'cache'.
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

    // get the filled cache-block and fill up the buffer over to at most 'max_len' bytes. Return the number of read bytes.
    fn read_segment(&mut self, buffer: &mut[u8], max_len: usize) -> usize {
        let block_idx = self.find_cached_block(self.position);
        let block = &self.cache[block_idx];
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

impl Read for s3_file {
    fn read(&mut self, buff: &mut [u8]) -> IOResult<usize> {
        let buff_len = buff.len();
        let mut read_len = 0;
        let mut window: &mut  [u8] = buff;
        while buff_len - read_len > 0 {
            let len = self.read_segment(window, buff_len - read_len);
            //shift the window forward (position has been updated already)
            window = &mut window[len..];
            read_len += len;
        }
        Ok(read_len)
    }
}


// temporarily included to test from MAIN
//#[cfg(test)]
pub mod tests {

    use aws_config::meta::region::RegionProviderChain;
    use aws_sdk_s3::{Client, Error, Region};
    //use aws_smithy_http::byte_stream::{ByteStream, AggregatedBytes};
    use uuid::Uuid;
    use futures::executor::block_on;
    use std::io::Read;

    use crate::s3_service;
    use crate::{s3_file, REGION};
    
    async fn setup() -> (Region, Client, String, String, String, String) {
        let region_provider = RegionProviderChain::first_try(Region::new(REGION));
        let region = region_provider.region().await.unwrap();
    
        let shared_config = aws_config::from_env().region(region_provider).load().await;
        let client = Client::new(&shared_config);
    
        let bucket_name = format!("{}{}", "doc-example-bucket-", Uuid::new_v4().to_string());
        let file_name = "./test_upload.txt".to_string();
        let key = "test file key name".to_string();
        let target_key = "target_key".to_string();
    
        (region, client, bucket_name, file_name, key, target_key)
    }


    pub async fn read_from_s3_aux(test_data: &[u8]) -> (Box<[u8]>, Box<[u8]>,Box<[u8]>) {
        let (region, client, bucket_name, file_name, object_name, target_key) = setup().await;
        println!("About to create {bucket_name}.");

        s3_service::create_bucket(&client, &bucket_name, region.as_ref()).await.expect("Failed to create bucket");
    
        // create the file for testing

        println!("About to create object {bucket_name}::{object_name}.");
        s3_service::upload_object(&client, &bucket_name, &file_name, &object_name, test_data).await.expect("Failed to create Object in bucket");
        println!("Created s3-object");

        // test 1
        let mut s3file_1 = s3_file::new(bucket_name.to_owned(), object_name.clone(), 10);

        println!("Read s3-object");
        let buff_len = 10;
        let mut buff1: Box<[u8]> = vec![0;buff_len].into_boxed_slice();
        let mut buff2: Box<[u8]> = vec![0;buff_len+7].into_boxed_slice();
        let mut buff3: Box<[u8]> = vec![0;buff_len].into_boxed_slice();

        // move position to 20  (start of "Hello World")
        s3file_1.position = 10;
        s3file_1.read(&mut buff1).expect("Failed to read S3-object (buff_1)");
        s3file_1.read(&mut buff2).expect("Failed to read S3-object (buff_2)");
        s3file_1.read(&mut buff3).expect("Failed to read S3-object (buff_3)");

        (buff1, buff2, buff3)
    }
    
    #[tokio::test]
    //#[test]
    async fn read_from_s3() {
        //write a dummy_test_file to a bucket
        //let results = block_on(read_from_s3_aux(s3_service::UPLOAD_CONTENT));
        println!("About to enter async function.");
        let results = read_from_s3_aux(s3_service::UPLOAD_CONTENT).await;
        
        assert_eq!(results.0.as_ref(), b"\nabcdefgh\n");
        assert_eq!(&results.1.as_ref(), b"Hello world!\n\nAnd");
        assert_eq!(results.2.as_ref(), b" a whole l");
    }
}
