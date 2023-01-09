use std::{
    io::Write,
    time::Instant
};
use futures::executor::block_on;
use s3_file::{s3_aux, get_region_client, S3Writer};
use uuid::Uuid;

const TEST_BUCKET_PREFIX: &str = "doc-example-bucket-";

fn create_test_bucket() -> String {
    let bucket_name = format!("{}{}", TEST_BUCKET_PREFIX, Uuid::new_v4().to_string());

    let (region, client) = block_on(get_region_client());

    block_on(s3_aux::create_bucket(&client, &bucket_name, region.as_ref()))
        .expect("Failed to create bucket");

    bucket_name
}

const NUM_ALPHA: usize = 200_000;

const THOUSAND_BLOCKS: bool = false;

#[tokio::main]
async fn main() {

    let bucket_name = create_test_bucket();
    let object_name = "alphabeth".to_owned();

    println!("About to create {bucket_name}:{object_name}");

    let mut s3_writer = S3Writer::new(bucket_name, object_name, 1024*1024*5+1);  // minimal part-size is 5Mb

    let alphabeth = b"abcdefghijklmnopqrstuvwxyz\n";

    let timer = Instant::now();
    let timer_2 = Instant::now();

    if THOUSAND_BLOCKS {
        let mut content = [0u8; NUM_ALPHA*27/1000];
        for i in 0..(NUM_ALPHA/1000) {
            let start = i * 27;
            let target_region = &mut content[start..(start+27)];
    
            target_region.clone_from_slice(alphabeth);
        }

        println!("==>  Building data Duration  {:?}", timer.elapsed());

        let mut num_written = 0;
        for _i in 0..1000 {
            match s3_writer.write(&content) {
                Ok(res) => num_written += res,
                Err(err) => eprintln!("write failed with {err:?}")
            }    
        }
        println!("Succesfully written {num_written} bytes");
        println!("==>  Written all data Duration  {:?}", timer.elapsed());
    
    } else {
        let mut content = [0u8; NUM_ALPHA*27];
        for i in 0..NUM_ALPHA {
            let start = i * 27;
            let target_region = &mut content[start..(start+27)];
    
            target_region.clone_from_slice(alphabeth);
        }

        println!("==>  Building data Duration  {:?}", timer.elapsed());
        println!("==>  Building data Duration-2  {:?}", timer.elapsed());

        match s3_writer.write(&content) {
            Ok(res) => println!("Succesfully written {res} bytes"),
            Err(err) => eprintln!("write failed with {err:?}")
        }

        println!("==>  Written all data Duration  {:?}", timer.elapsed());
        println!("==>  Written all data Duration  SECOND {:?}", timer_2.elapsed());
    
    }

    println!("Reported length of object: {}", s3_writer.get_length());

    match s3_writer.flush() {
        Ok(()) => (),
        Err(err) => eprintln!("Flush failed: {err:?}")
    };

    println!("==>  Flushed all data Duration  {:?}", timer.elapsed());

    println!("Reported length of object (after Flush): {}", s3_writer.get_length());


    if let Err(err) = s3_writer.close() {
        eprintln!("\n======================\n{err:?}\n");
    };


    println!("Closed the object. Check S3 if the object now exists");
}