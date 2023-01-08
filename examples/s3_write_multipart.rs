use std::{
    io::Write
};
use futures::executor::block_on;
use S3_file::{s3_service, get_region_client, S3Writer};
use uuid::Uuid;

const TEST_BUCKET_PREFIX: &str = "doc-example-bucket-";

fn create_test_bucket() -> String {
    let bucket_name = format!("{}{}", TEST_BUCKET_PREFIX, Uuid::new_v4().to_string());

    let (region, client) = block_on(get_region_client());

    block_on(s3_service::create_bucket(&client, &bucket_name, region.as_ref()))
        .expect("Failed to create bucket");

    bucket_name
}

const NUM_ALPHA: usize = 200000;
#[tokio::main]
async fn main() {

    let bucket_name = create_test_bucket();
    let object_name = "alphabeth".to_owned();

    println!("About to create {bucket_name}:{object_name}");

    let mut s3_writer = S3Writer::new(bucket_name, object_name, 1024*1024*5+1);  // minimal part-size is 5Mb

    let alphabeth = b"abcdefghijklmnopqrstuvwxyz\n";

    let mut content = [0u8; NUM_ALPHA*27];
    for i in 0..NUM_ALPHA {
        let start = i * 27;
        let target_region = &mut content[start..(start+27)];

        target_region.clone_from_slice(alphabeth);
    }

    match s3_writer.write(&content) {
        Ok(res) => println!("Succesfully written {res} bytes"),
        Err(err) => println!("write failed with {err:?}")
    }

    println!("Reported length of object: {}", s3_writer.get_length());

    s3_writer.flush();


    println!("Reported length of object (after Flush): {}", s3_writer.get_length());


    if let Err(err) = s3_writer.close() {
        eprintln!("\n======================\n{err:?}\n");
    };

    println!("Closed the object. Check S3 if the object now exists");
}