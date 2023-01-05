/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

 use aws_config::meta::region::RegionProviderChain;
 use aws_sdk_s3::{Client, Error, Region};
 //use aws_smithy_http::byte_stream::{ByteStream, AggregatedBytes};
 use uuid::Uuid;
 use std::str;
 use std::time::Instant;


 // needed for Lambda variant only
use lambda_runtime::{run, service_fn, Error as LmdError, LambdaEvent};
use serde::{Deserialize, Serialize};


mod s3_service;


async fn setup() -> (Region, Client, String, String, String, String) {
    let region_provider = RegionProviderChain::first_try(Region::new("us-west-2"));
    let region = region_provider.region().await.unwrap();

    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    let bucket_name = format!("{}{}", "doc-example-bucket-", Uuid::new_v4().to_string());
    let file_name = "./test_upload.txt".to_string();
    let key = "test file key name".to_string();
    let target_key = "target_key".to_string();

    (region, client, bucket_name, file_name, key, target_key)
}



async fn run_s3_operations(
    region: Region,
    client: Client,
    bucket_name: String,
    file_name: String,
    key: String,
    target_key: String,
) -> Result<String, Error> {
    let mut msgs = Vec::new();
    msgs.push("\nResult of S3_operations:".to_owned());

    let start = Instant::now();
    s3_service::create_bucket(&client, &bucket_name, region.as_ref()).await?;
    let now = Instant::now();
    s3_service::upload_object(&client, &bucket_name, &file_name, &key, &s3_service::UPLOAD_CONTENT).await?;
    let duration = now.elapsed();
    msgs.push(format!("Upload of file took: {:?}", &duration));
    let now = Instant::now();
    let dl = s3_service::download_object(&client, &bucket_name, &key, Some("bytes=20-35".to_owned())).await;
    let duration = now.elapsed();
    //println!("\nraw dl = {:?}\n\tduration: {:?}", &dl, &duration);
    // println!(" result.accept_ranges = {:?}", dl.accept_ranges());
    // println!(" result.content_length = {:?}", dl.content_length());
    let bytes = dl.body.collect().await.expect("Failed to retrieve bytes");
    // //println!("string dl = {}", str::from_utf8(&(bytes.into_bytes())).expect("Failed to convert to string"));
    msgs.push(format!("contents of download dl = {:?}.   Duration: {:?}", &bytes, &duration));
    s3_service::copy_object(&client, &bucket_name, &key, &target_key).await?;
    s3_service::list_objects(&client, &bucket_name).await?;
    s3_service::delete_objects(&client, &bucket_name).await?;
    s3_service::delete_bucket(&client, &bucket_name).await?;

    msgs.push(format!("Total time spend on S3-operations: {:?}", start.elapsed()));

    Ok(msgs.join("\n\t"))
}

// TMP:  remove/comment the feature-flag #[cfg(test)]  in file lib.rs
// use S3_file::tests;

// // normal main used for console operation
// #[tokio::main]
// async fn main() -> Result<(), Error> {
//     // tracing_subscriber::fmt()
//     //     .with_max_level(tracing::Level::INFO)
//     //     // disable printing the name of the module in every log line.
//     //     .with_target(false)
//     //     // disabling time is handy because CloudWatch will add the ingestion time.
//     //     .without_time()
//     //     .init();

//     // run tests from main
//     println!("About to enter async function.");
//     let results = tests::read_from_s3_aux(s3_service::UPLOAD_CONTENT).await;
//     println!("\n----------------\nresults are results.0={:?} and as string: {:?}", &results.0, str::from_utf8(&results.0));
//     println!("results are results.0={:?} and as string: {:?}", &results.1, str::from_utf8(&results.1));
//     println!("results are results.0={:?} and as string: {:?}", &results.2, str::from_utf8(&results.2));
//     println!(" Read_from_s3_aux ready!!!\n");

//     Ok(())
// }



/// This is a made-up example. Requests come into the runtime as unicode
/// strings in json format, which can map to any structure that implements `serde::Deserialize`
/// The runtime pays no attention to the contents of the request payload.
#[derive(Deserialize)]
struct Request {
}


/// This is a made-up example of what a response structure may look like.
/// There is no restriction on what it can be. The runtime requires responses
/// to be serialized into json. The runtime pays no attention
/// to the contents of the response payload.
#[derive(Serialize)]
struct Response {
    req_id: String,
    msg: String,
}

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
async fn function_handler(event: LambdaEvent<Request>) -> Result<Response, LmdError> {


    let (region, client, bucket_name, file_name, key, target_key) = setup().await;

    let resp_msg = match run_s3_operations(region, client, bucket_name, file_name, key, target_key).await {
        Ok(msgs) => msgs,
        Err(msg) => format!("FAILURE: {}", msg)
    };

    // Prepare the response
    let resp = Response {
        req_id: event.context.request_id,
        msg: resp_msg,
    };

    // Return `Response` (it will be serialized to JSON automatically by the runtime)
    Ok(resp)
}

//Lambda-main
#[tokio::main]
async fn main() -> Result<(), LmdError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await
}
