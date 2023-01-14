use s3_file;
use aws_sdk_s3::Error;



/// example that recursively deletes a series of buckets if they have a specific prefix.
/// Used to clean up after a series of tests that create filled buckets.
#[tokio::main]
async fn main() -> Result<(), Error> {
    // tracing_subscriber::fmt()
    //     .with_max_level(tracing::Level::INFO)
    //     // disable printing the name of the module in every log line.
    //     .with_target(false)
    //     // disabling time is handy because CloudWatch will add the ingestion time.
    //     .without_time()
    //     .init();

    // run tests from main

    let client = s3_file::get_client().await;
    if let Err(err) = s3_file::delete_buckets_with_prefix(&client, "doc-example-bucket").await {
        eprintln!("Error while deletion of buckets: {err:?}");
    };

    return Ok(());
}
