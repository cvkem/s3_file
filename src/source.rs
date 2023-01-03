
use aws_sdk_s3::{Client, Error, Region,
    types::ByteStream};
use aws_config::meta::region::RegionProviderChain;
use std::io::Result as IOResult;
use std::str;
use bytes::Bytes;
use futures::executor::block_on;

use crate::s3_service;

pub const REGION: &str = "eu-central-1";



#[async_trait]
pub trait GetBytes  {
    async fn get_bytes(start: usize, end: usize) -> Bytes;
}

async fn get_client() -> Client {
    let region_provider = RegionProviderChain::first_try(Region::new(REGION));
    let region = region_provider.region().await.unwrap();

    let shared_config = aws_config::from_env().region(region_provider).load().await;
    Client::new(&shared_config)
}


pub struct ObjectSource {
    client: Client,
    pub bucket: String,
    pub object: String,
    length: Option<usize>,
}

impl ObjectSource {
    pub fn new(bucket: String, object: String) -> Self {
        Self{client: block_on(get_client()), 
            bucket, 
            object, 
            length: None}
    }

    /// get the length when available, and otherwise compute it.
    pub fn get_length(&mut self) -> IOResult<u64> {
        let length = self.length.get_or_insert( //|| 
            block_on(async {
                s3_service::head_object(&self.client, &self.bucket, &self.object)
                .await
                .content_length() as usize}));
        Ok(*length as u64)
    }

}

#[async_trait]
impl GetBytes for ObjectSource {

    async fn get_bytes(block_start: usize, block_end: usize) -> Bytes {
        let range = format!("bytes={block_start}-{block_end}");
        // should be seperate function to read bytes for a cache-block
        let get_obj_output = s3_service::download_object(&self.client, &self.bucket, &self.object, Some(range)).await;
        println!("Received object {:?}", get_obj_output);
        // set length of full object when not readily available, as we get this information free of charge here.
        _ = self.length.get_or_insert(get_obj_output.content_length() as usize);
        let agg_bytes = get_obj_output.body.collect().await.expect("Failed to read data");
        println!("Received bytes {:?}", agg_bytes);
        // turn into bytes and take a (ref-counted) full slice out of it (reuse of same buffer)
        // Operating on AggregatedBytes directy would be more memory efficient (however, working with non-continguous memory in that case)
        let data = agg_bytes.into_bytes();
    }
}
