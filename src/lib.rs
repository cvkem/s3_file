pub use client::{get_client, get_region_client};
pub use s3_service::delete_buckets_with_prefix;
pub use s3_file::S3File;

mod client;
pub mod s3_service;
mod lru_cache;
mod source;
mod s3_file;



#[cfg(test)]
pub mod tests {

    // use aws_config::meta::region::RegionProviderChain;
    use aws_sdk_s3::{Client, Region};
    //use aws_smithy_http::byte_stream::{ByteStream, AggregatedBytes};
    use uuid::Uuid;
 //   use futures::executor::block_on;
    use std::io::{Read, Seek, SeekFrom};

    use crate::{
        s3_service,
        s3_file::S3File, 
        client::get_region_client};
    
    async fn setup() -> (Region, Client, String, String, String, String) {
    
        let (region, client) = get_region_client().await;

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


    // // create a test-input file and run the test.
    // pub async fn read_from_s3_aux(test_data: &[u8]) -> (Box<[u8]>, Box<[u8]>,Box<[u8]>) {
    //     let (region, client, bucket_name, file_name, object_name, target_key) = setup().await;
    //     s3_service::create_bucket(&client, &bucket_name, region.as_ref()).await.expect("Failed to create bucket");
    
    //     // create the file for testing
    //     s3_service::upload_object(&client, &bucket_name, &file_name, &object_name, test_data).await.expect("Failed to create Object in bucket");

    //     // the actual test.
    //     test_read_S3File_aux(Some(&bucket_name), &object_name)
    // }
    
}
