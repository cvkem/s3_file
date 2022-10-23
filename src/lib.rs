
use aws_sdk_s3::{Client, Error, Region,
    types::ByteStream};
use aws_config::meta::region::RegionProviderChain;
use std::io::{Read, Result as IOResult, Seek, SeekFrom, Error as IOError, ErrorKind as IOErrorKind};
use std::time::Instant;
use std::any::type_name;
use std::ptr;
use std::str;
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

pub struct S3File {
    client: Client,
    pub bucket: String,
    pub object: String,
    position: usize,
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

impl S3File {

    
    // create a new S3File with an LRU-cache to support fast (sequential) read operations
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

    // append a block to the cache that contains the byte start and append it to the cache
    fn get_block_from_store(&mut self, start: usize) -> usize {
        let block_start = (start / self.block_size) * self.block_size;
        //let end_block = cmp::min(block_start + self.block_size, self.get_length());
        let block_end = block_start + self.block_size - 1;  // end is inclusive

        // create the block and fill it with data
        let range = format!("bytes={block_start}-{block_end}");
        println!("\nAbout to fetch object {}::{} for range='{}'", &self.bucket, &self.object, &range);
        let f = async {
            let get_obj_output = s3_service::download_object(&self.client, &self.bucket, &self.object, Some(range)).await;
            println!("Received object {:?}", get_obj_output);
            // set length when nog readily available, as we get this information free of charge here.
            _ = self.length.get_or_insert(get_obj_output.content_length() as usize);
            let agg_bytes = get_obj_output.body.collect().await.expect("Failed to read data");
            println!("Received bytes {:?}", agg_bytes);
            // turn into bytes and take a (ref-counted) full slice out of it (reuse of same buffer)
            // Operating on AggregatedBytes directy would be more memory efficient (however, working with non-continguous memory in that case)
            let data = agg_bytes.into_bytes();
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

    // get the length when available, and otherwise compute it.
    pub fn get_length(&mut self) -> IOResult<u64> {
        let length = self.length.get_or_insert( //|| 
            block_on(async {
                s3_service::head_object(&self.client, &self.bucket, &self.object)
                .await
                .content_length() as usize}));
        Ok(*length as u64)
    }
}

impl Read for S3File {
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

impl Seek for S3File {
    fn seek(&mut self, pos: SeekFrom) -> IOResult<u64> {
        let new_pos: i64 = match pos {
            SeekFrom::Start(upos) =>  upos as i64,
            SeekFrom::Current(ipos) =>   self.position as i64 + ipos,
            // SeekFrom::End(ipos) -> {
            //     match self.get_length() {
            //         Ok(len) -> len as i64 + ipos,
            //         Err(e) -> return Err(e)
            //     }
            SeekFrom::End(ipos) => self.get_length()? as i64 + ipos
            }; 

        // check the validity of the new position
        if  new_pos < 0 {
            return Err(IOError::new(IOErrorKind::InvalidInput, "Position should not before 0."));
        } else if new_pos > self.get_length()? as i64 {
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

// temporarily included to test from MAIN
//#[cfg(test)]
pub mod tests {

    use aws_config::meta::region::RegionProviderChain;
    use aws_sdk_s3::{Client, Error, Region};
    //use aws_smithy_http::byte_stream::{ByteStream, AggregatedBytes};
    use uuid::Uuid;
    use futures::executor::block_on;
    use std::io::{Read, Seek, SeekFrom};

    use crate::s3_service;
    use crate::{S3File, REGION};
    
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

    const DEFAULT_BUCKET: &str = "doc-example-bucket-f895604e-164e-4587-9d6c-bc3b7da55fa2";
    const DEFAULT_OBJECT: &str = "test file key name";


    pub fn test_read_S3File_aux(bucket_name: Option<&str>, object_name: &str) -> (Box<[u8]>, Box<[u8]>,Box<[u8]>) {
        // use a default bucket if none is specified
        let bucket_name = bucket_name.unwrap_or(&DEFAULT_BUCKET);
        // test 1
        let mut s3file_1 = S3File::new(bucket_name.to_owned(), object_name.to_string(), 10);

        let buff_len = 10;
        let mut buff1: Box<[u8]> = vec![0;buff_len].into_boxed_slice();
        let mut buff2: Box<[u8]> = vec![0;buff_len+7].into_boxed_slice();
        let mut buff3: Box<[u8]> = vec![0;buff_len].into_boxed_slice();

        // move position to 10  (start of "Hello World" is at 20)
        s3file_1.position = 10;
        s3file_1.read(&mut buff1).expect("Failed to read S3-object (buff_1)");  // read 10 bytes
        s3file_1.read(&mut buff2).expect("Failed to read S3-object (buff_2)");  // read 17 bytes
        s3file_1.read(&mut buff3).expect("Failed to read S3-object (buff_3)");  // read 10 bytes

        (buff1, buff2, buff3)

    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_S3File() {
        let (b1, b2, b3) = test_read_S3File_aux(None, &DEFAULT_OBJECT);
        println!("\tb1={:?}\n\tb2={:?}\n\tb3={:?}", b1, b2, b2);
        assert_eq!(b1.as_ref(), b"\nabcdefgh\n");
        assert_eq!(b2.as_ref(), b"Hello world!\n\nAnd");
        assert_eq!(b3.as_ref(), b" a whole l");

    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_seek_S3File() {
        let mut s3file_1 = S3File::new(DEFAULT_BUCKET.to_owned(), DEFAULT_OBJECT.to_owned(), 15);

        let buff_len = 36;
        let mut buff1: Box<[u8]> = vec![0;buff_len].into_boxed_slice();
        let mut buff2: Box<[u8]> = vec![0;buff_len+7].into_boxed_slice();
        let mut buff3: Box<[u8]> = vec![0;buff_len].into_boxed_slice();

        // move position to 30  from the end and read 30
        s3file_1.seek(SeekFrom::End(-1 * buff_len as i64));
        s3file_1.read(&mut buff1).expect("Failed to read S3-object (buff_1)");  // read 10 bytes
    
//        println!("\tbuff1={:?}\n\tb2={:?}\n\tb3={:?}", b1, b2, b2);
        println!("\n###################\n\tbuff1={:?}\n", buff1);
        assert_eq!(buff1.as_ref(), b"Nunc nec tristique diam.\nTouch test.");

    }


    // create a test-input file and run the test.
    pub async fn read_from_s3_aux(test_data: &[u8]) -> (Box<[u8]>, Box<[u8]>,Box<[u8]>) {
        let (region, client, bucket_name, file_name, object_name, target_key) = setup().await;
        s3_service::create_bucket(&client, &bucket_name, region.as_ref()).await.expect("Failed to create bucket");
    
        // create the file for testing
        s3_service::upload_object(&client, &bucket_name, &file_name, &object_name, test_data).await.expect("Failed to create Object in bucket");

        // the actual test.
        test_read_S3File_aux(Some(&bucket_name), &object_name)
    }
    
}
