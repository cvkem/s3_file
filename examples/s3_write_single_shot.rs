use std::{
    io::Write,
    time::Instant
};
use futures::executor::block_on;
use s3_file::{
    async_bridge,
    s3_aux, 
    get_region_client, 
    S3Writer};
use uuid::Uuid;

const TEST_BUCKET_PREFIX: &str = "doc-example-bucket-";


fn create_test_bucket() -> String {
    let bucket_name = format!("{}{}", TEST_BUCKET_PREFIX, Uuid::new_v4().to_string());

    // for some reason this function does not need a runtime yet. The S3_aux::create_bucket does need it.
    let (region, client) = block_on(get_region_client());
    let create_bucket = s3_aux::create_bucket(&client, &bucket_name, region.as_ref());

    async_bridge::run_async(create_bucket).unwrap();

    bucket_name
}

// do not make this variable much larger, as this block is passed over the stack and blows up the default stack of the main thread.
const NUM_ALPHA: usize = 250;

const NUM_BLOCKS: i32 = 2;


//#[tokio::main]
// async fn main() {
fn main() {

    // use console_subscriber;
    // console_subscriber::init();

    let bucket_name = create_test_bucket();
    let object_name = "alphabeth".to_owned();

    println!("About to create {bucket_name}:{object_name}");

    let mut s3_writer = S3Writer::new(bucket_name, object_name, 1024*1024*5+1);  // minimal part-size is 5Mb

    let alphabeth = b"abcdefghijklmnopqrstuvwxyz\n";

    let timer = Instant::now();

    let mut num_written = 0;

    for i in 0..NUM_BLOCKS {
        println!("Writing block {i}");
        match s3_writer.write(alphabeth) {
            Ok(res) => num_written += res,
            Err(err) => eprintln!("write failed with {err:?}")
        }        
    }

    // println!("Writen bytes= {num_written}");
    // println!("Reported length of object: flushed_bytes={}", s3_writer.get_flushed_bytes());

    // match s3_writer.flush() {
    //     Ok(()) => (),
    //     Err(err) => eprintln!("Flush failed: {err:?}")
    // };

    println!("==>  Flushed all data Duration  {:?}", timer.elapsed());
    println!("Reported length of object (after Flush): flushed_bytes={}", s3_writer.get_flushed_bytes());

    if let Err(err) = s3_writer.close() {
        eprintln!("\n======================\n{err:?}\n");
    };

    println!("Total duration of writing:  {:?}\nClosed the object. Check S3 if the object now exists", timer.elapsed());
}