use std::{
    str,
    io::Read};
use S3_file::{self, get_region_client, s3_service, S3Reader};
use aws_sdk_s3::{Client, Error, Region};
use uuid::Uuid;


const DEFAULT_BUCKET: &str = "doc-example-bucket-f895604e-164e-4587-9d6c-bc3b7da55fa2";
const DEFAULT_OBJECT: &str = "test file key name";


async fn setup() -> (Region, Client, String, String, String, String) {
    
    let (region, client) = get_region_client().await;

    let bucket_name = format!("{}{}", "doc-example-bucket-", Uuid::new_v4().to_string());
    let file_name = "./test_upload.txt".to_string();
    let key = "test file key name".to_string();
    let target_key = "target_key".to_string();

    (region, client, bucket_name, file_name, key, target_key)
}




pub fn test_read_S3File_aux(bucket_name: Option<&str>, object_name: &str) -> (Box<[u8]>, Box<[u8]>,Box<[u8]>) {
    // use a default bucket if none is specified
    let bucket_name = bucket_name.unwrap_or(&DEFAULT_BUCKET);
    // test 1
    let mut s3file_1 = S3Reader::new(bucket_name.to_owned(), object_name.to_string(), 10);

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


// create a test-input file and run the test.
pub async fn read_from_s3_aux(test_data: &[u8]) -> (Box<[u8]>, Box<[u8]>,Box<[u8]>) {
    let (region, client, bucket_name, file_name, object_name, target_key) = setup().await;
    s3_service::create_bucket(&client, &bucket_name, region.as_ref()).await.expect("Failed to create bucket");

    // create the file for testing
    s3_service::upload_object(&client, &bucket_name, &file_name, &object_name, test_data).await.expect("Failed to create Object in bucket");

    // the actual test and return three byte-arrays.
    test_read_S3File_aux(Some(&bucket_name), &object_name)
}


#[tokio::main]
async fn main() -> Result<(), Error> {
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::INFO)
    //     // disable printing the name of the module in every log line.
    //     .with_target(false)
    //     // disabling time is handy because CloudWatch will add the ingestion time.
    //     .without_time()
    //     .init();


    println!("About to enter async function.");
    let results = read_from_s3_aux(s3_service::UPLOAD_CONTENT).await;
    println!("\n----------------\nresults are results.0={:?} and as string: {:?}", &results.0, str::from_utf8(&results.0));
    println!("results are results.0={:?} and as string: {:?}", &results.1, str::from_utf8(&results.1));
    println!("results are results.0={:?} and as string: {:?}", &results.2, str::from_utf8(&results.2));
    println!(" Read_from_s3_aux ready!!!\n");

    Ok(())
}

