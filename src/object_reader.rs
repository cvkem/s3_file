
use aws_sdk_s3::Client;
use std::{
    io::Result as IOResult,
    sync::{
        MutexGuard}};
use bytes::Bytes;
use futures::executor::block_on;
use async_trait::async_trait;

use crate::{client, s3_aux, async_bridge};



#[async_trait]
pub trait GetBytes  {
    async fn get_bytes(&self, start: usize, end: usize) -> Bytes;
}


#[derive(Debug)]
pub struct ObjectReader {
    client: Client,
    pub bucket: String,
    pub object: String,
    length: Option<usize> 
}

impl ObjectReader {
    pub fn new(bucket: String, object: String) -> Self {
        Self{client: block_on(client::get_client()), 
            bucket, 
            object, 
            length: None}
    }

    pub fn get_object_name(&self) -> String {
        format!("{}:{}", self.bucket, self.object)
    }

    /// get the length when available, and otherwise compute it.
    pub fn get_length(&mut self) -> IOResult<u64> {
        let length_ref = self.length.get_or_insert_with(|| {
            async_bridge::run_async(
                async {
                    s3_aux::head_object(&self.client, &self.bucket, &self.object)
                    .await
                    .content_length() as usize})});
        Ok(*length_ref as u64)
    }

    // pub fn close(self) -> IOResult<()> {
    //     // TODO: implement it
    // }
}

#[async_trait]
impl GetBytes for MutexGuard<'_, ObjectReader> {

    async fn get_bytes(&self, block_start: usize, block_end: usize) -> Bytes {
        let range = format!("bytes={block_start}-{block_end}");
        // should be seperate function to read bytes for a cache-block
        let get_obj_output = s3_aux::download_object(&self.client, &self.bucket, &self.object, Some(range)).await;
        // set length of full object when not readily available, as we get this information free of charge here.
// TODO: add next line again by using internal mutability as self is not mutable here.
//        _ = self.length.get_or_insert_with(|| get_obj_output.content_length() as usize);
        let agg_bytes = get_obj_output.body.collect().await.expect("Failed to read data");
        // turn into bytes and take a (ref-counted) full slice out of it (reuse of same buffer)
        // Operating on AggregatedBytes directy would be more memory efficient (however, working with non-continguous memory in that case)
        let data = agg_bytes.into_bytes();
        data
    }
}
