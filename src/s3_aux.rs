/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

// snippet-start:[rust.example_code.s3.scenario_getting_started.lib]


use aws_sdk_s3::model::{
    BucketLocationConstraint, CreateBucketConfiguration, Delete, ObjectIdentifier,
};
use aws_sdk_s3::output::{GetObjectOutput, HeadObjectOutput, ListObjectsV2Output};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Error};
use std::str;



#[allow(dead_code)]
// snippet-start:[rust.example_code.s3.basics.delete_bucket]
pub async fn delete_bucket(client: &Client, bucket_name: &str) -> Result<(), Error> {
    client.delete_bucket().bucket(bucket_name).send().await?;
    println!("Bucket deleted");
    Ok(())
}
// snippet-end:[rust.example_code.s3.basics.delete_bucket]

#[allow(dead_code)]
// snippet-start:[rust.example_code.s3.basics.delete_objects]
pub async fn delete_objects(client: &Client, bucket_name: &str) -> Result<usize, Error> {
    let objects = client.list_objects_v2().bucket(bucket_name).send().await?;

    let mut delete_objects: Vec<ObjectIdentifier> = vec![];
    for obj in objects.contents().unwrap_or_default() {
        let obj_id = ObjectIdentifier::builder()
            .set_key(Some(obj.key().unwrap().to_string()))
            .build();
        delete_objects.push(obj_id);
    }

    let num_obj = delete_objects.len();

    client
        .delete_objects()
        .bucket(bucket_name)
        .delete(Delete::builder().set_objects(Some(delete_objects)).build())
        .send()
        .await?;

    let objects: ListObjectsV2Output = client.list_objects_v2().bucket(bucket_name).send().await?;
    match objects.key_count {
        0 => Ok(num_obj),
        _ => Err(Error::Unhandled(Box::from(
            "There were still objects left in the bucket.",
        ))),
    }
}
// snippet-end:[rust.example_code.s3.basics.delete_objects]


#[allow(dead_code)]
// snippet-start:[rust.example_code.s3.basics.list_objects]
pub async fn list_objects(client: &Client, bucket_name: &str) -> Result<(), Error> {
    let objects = client.list_objects_v2().bucket(bucket_name).send().await?;
    println!("Objects in bucket:");
    for obj in objects.contents().unwrap_or_default() {
        println!("{:?}", obj.key().unwrap());
    }

    Ok(())
}
// snippet-end:[rust.example_code.s3.basics.list_objects]


pub async fn delete_buckets_with_prefix(client: &Client, bucket_prefix: &str) -> Result<(), Error> {
    let buckets = client.list_buckets().send().await?;

    for bucket in buckets.buckets().unwrap_or_default() {
        println!("bucket-info: {:?}", bucket);

        let bucket_name = bucket.name().unwrap();
        if bucket_name.starts_with(bucket_prefix) {
            match delete_objects(client, bucket_name).await {
                Ok(num_obj) => println!("Deleted {num_obj} objects from bucket {bucket_name}"),
                Err(err) => println!("Deleting bucket-contents of {bucket_name} resturned {err:?}")
            };

            match delete_bucket(client, bucket_name).await {
                Ok(_) => println!("Deleted bucket {bucket_name}"),
                Err(err) => println!("Deletion of bucket {bucket_name} failed with error {err:?}")
            };

        } else {
            println!("Bucket with name {bucket_name} does not have the expected prefix.");
        }
    };

    Ok(())
}


#[allow(dead_code)]
// snippet-start:[rust.example_code.s3.basics.copy_object]
pub async fn copy_object(
    client: &Client,
    bucket_name: &str,
    object_key: &str,
    target_key: &str,
) -> Result<(), Error> {
    let mut source_bucket_and_object: String = "".to_owned();
    source_bucket_and_object.push_str(bucket_name);
    source_bucket_and_object.push('/');
    source_bucket_and_object.push_str(object_key);

    client
        .copy_object()
        .copy_source(source_bucket_and_object)
        .bucket(bucket_name)
        .key(target_key)
        .send()
        .await?;

    Ok(())
}
// snippet-end:[rust.example_code.s3.basics.copy_object]


#[allow(dead_code)]
// snippet-start:[rust.example_code.s3.basics.download_object]
// snippet-start:[rust.example_code.s3.basics.get_object]
pub async fn download_object(client: &Client, bucket_name: &str, key: &str, range: Option<String>) -> GetObjectOutput {
    let prep_resp = client
        .get_object()
        //.range("bytes=20-".to_owned())
        .set_range(range)
        .bucket(bucket_name)
        .key(key);
//    println!("\nPrepared Download request = {:?}\n", &prep_resp);
    let resp = prep_resp    
        .send()
        .await;
    resp.unwrap()
}
// snippet-end:[rust.example_code.s3.basics.get_object]
// snippet-end:[rust.example_code.s3.basics.download_object]


#[allow(dead_code)]
// get the head of an objects. Mainly needed to compute the length of the S3-object
pub async fn head_object(client: &Client, bucket_name: &str, key: &str) -> HeadObjectOutput {
    let resp = client
        .head_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await;
    resp.unwrap()
}

#[allow(dead_code)]
// TODO: remove: not used anymore
pub const UPLOAD_CONTENT: &[u8] = b"0123456789
abcdefgh
Hello world!

And a whole lot more information:

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Mauris nulla dolor, varius vel vehicula vitae, dapibus id tellus. Vivamus nisl risus, pretium in nisi non, venenatis rhoncus ligula. Sed pharetra nibh nulla. Integer vitae mollis nisi. Nunc ante nulla, cursus id dolor sit amet, suscipit molestie est. Vestibulum sollicitudin fermentum arcu ut dictum. Sed finibus feugiat libero, sit amet fermentum ex consequat auctor. Donec varius fringilla sagittis. Sed gravida efficitur erat et viverra. Nulla malesuada metus vitae lacus malesuada, at faucibus velit tincidunt. Cras fermentum blandit purus. Integer rutrum semper lorem, rutrum aliquet elit egestas ac. Integer tincidunt sem nec turpis aliquet consectetur. Nunc accumsan est leo, ut tincidunt turpis interdum in. Ut finibus nibh et ullamcorper pharetra.
Vivamus quis dictum ipsum. Proin congue vulputate elit rhoncus congue. Mauris elementum libero mollis, mattis nibh eu, commodo erat. Duis sodales feugiat diam, ut tempor purus viverra eu. Vestibulum sodales sit amet eros quis placerat. Phasellus venenatis condimentum nisl eget tristique. Suspendisse potenti. Cras a orci nec elit pulvinar condimentum at et diam. Mauris faucibus aliquam posuere. Donec accumsan, metus ac scelerisque iaculis, felis enim ullamcorper ante, quis tristique elit justo et ex. Donec et velit pharetra, viverra lacus vitae, laoreet elit.
Duis sodales odio velit, nec ornare leo venenatis ut. Pellentesque in libero sit amet libero vestibulum lobortis. Vivamus laoreet ex eget mi suscipit, vitae volutpat felis iaculis. Etiam rhoncus ac arcu nec commodo. Phasellus posuere, mi eget egestas tincidunt, orci tellus placerat turpis, sit amet blandit nibh magna eu est. Nunc ultrices tincidunt rhoncus. Praesent faucibus id augue quis laoreet. Maecenas ac pellentesque eros, id pellentesque magna. Integer faucibus auctor ante. Mauris vitae pharetra erat. Nulla facilisi. Fusce ac ex libero. Duis posuere lacus lectus, ut varius sapien efficitur eget. Nunc enim urna, congue non tristique id, consequat eget nisi. Nulla in massa sit amet nisi rhoncus tempor sagittis id erat.
Donec in accumsan odio. Integer faucibus velit posuere sem commodo tincidunt. Fusce facilisis ex eget nisl feugiat pulvinar. Curabitur dolor diam, tempus in ligula ut, feugiat cursus elit. Sed et neque molestie, vehicula diam sed, ultricies justo. Maecenas rutrum pharetra sapien eu interdum. Donec vulputate, massa quis malesuada eleifend, ex arcu viverra magna, sit amet volutpat sem risus sit amet dui. Sed ultricies at eros at consequat. Morbi fringilla tristique mauris vel iaculis. Pellentesque ornare dictum finibus. Fusce aliquet odio a blandit interdum.
Donec mollis finibus metus in cursus. In blandit ornare purus. Vestibulum a ipsum diam. Curabitur vel iaculis diam, id egestas ligula. Morbi condimentum imperdiet tellus. Aenean ut ligula nulla. Nam quis auctor odio. Sed eget blandit magna, sit amet consectetur ex. Quisque fermentum nunc at nisi dictum molestie. Ut tempus fermentum ipsum, vel aliquet sem rutrum quis. Nunc nec tristique diam.
Touch test.";


pub async fn upload_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    body: &[u8]
) -> Result<(), Error> {
//    let body = ByteStream::from_static(UPLOAD_CONTENT);
    let body = ByteStream::from(Vec::from(body));
    client
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        //.body(body.unwrap())
        .body(body)
        .send()
        .await?;

    println!("Uploaded to bucket: {bucket_name}:{object_name}");
    Ok(())
}
// snippet-end:[rust.example_code.s3.basics.put_object]
// snippet-end:[rust.example_code.s3.basics.upload_object]

#[allow(dead_code)]
// snippet-start:[rust.example_code.s3.basics.create_bucket]
pub async fn create_bucket(client: &Client, bucket_name: &str, region: &str) -> Result<(), Error> {
    let constraint = BucketLocationConstraint::from(region);
    let cfg = CreateBucketConfiguration::builder()
        .location_constraint(constraint)
        .build();
    client
        .create_bucket()
        .create_bucket_configuration(cfg)
        .bucket(bucket_name)
        .send()
        .await?;
    println!("Creating bucket named: {bucket_name}");
    Ok(())
}
// snippet-end:[rust.example_code.s3.basics.create_bucket]
// snippet-end:[rust.example_code.s3.scenario_getting_started.lib]