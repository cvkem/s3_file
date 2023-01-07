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
