use aws_sdk_s3::{
    Client,
    types::ByteStream};
use std::{
    io::{self, Result as IOResult},
    mem};
use bytes::Bytes;
use async_trait::async_trait;
use futures::executor::block_on;

use crate::client;

use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::output::CreateMultipartUploadOutput;



pub struct ObjectWriter {
    client: Client,
    pub bucket_name: String,
    pub object_name: String,
    upload_id: Option<String>,
    last_part_nr: i32,
    upload_parts: Vec<CompletedPart>,
    length: usize,
    closed: bool
}


#[async_trait]
pub trait ObjectWriterAux {
    async fn create_multipart_upload(&mut self) -> IOResult<()>;
    async fn upload_part(&mut self, part:  Bytes) -> IOResult<()>;
    async fn close(&mut self) -> IOResult<()>;
}

impl ObjectWriter {

    pub fn new(bucket: String, object: String) -> Self {
        Self{client: block_on(client::get_client()), 
            bucket_name: bucket, 
            object_name: object,
            upload_id: None,
            last_part_nr: 0,
            upload_parts: Vec::new(),
            length: 0,
            closed: false
        }
    }

    /// return the next part_number, which will start counting from 1.
    fn get_next_part_nr(&mut self) -> i32 {
        self.last_part_nr += 1;
        return self.last_part_nr
    }
}


#[async_trait]
impl ObjectWriterAux for ObjectWriter {

    async fn create_multipart_upload(&mut self) -> IOResult<()> {
        let multipart_upload_res: CreateMultipartUploadOutput = self.client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(&self.object_name)
            .send()
            .await
            .unwrap();

        match multipart_upload_res.upload_id() {
            Some(upload_id) => self.upload_id = Some(upload_id.to_owned()),
            None => return Err(io::Error::new(io::ErrorKind::Other, "Failed to create multi-part upload"))
        };

        Ok(())
    }

    async fn upload_part(&mut self, part:  Bytes) -> IOResult<()> {
        let part_len = part.len();
        self.length += part_len;

        let stream = ByteStream::from(part);

        let upload_id = self.upload_id.as_ref().unwrap_or_else(|| panic!("No upload-id set, so the multi-part upload has not been started. Call 'create_multipart_upload' first")).clone();
        let part_number = self.get_next_part_nr();

        println!("Writing block {part_number} of size {part_len}");

        let upload_part_res = match self.client
            .upload_part()
            .key(&self.object_name)
            .bucket(&self.bucket_name)
            .upload_id(upload_id)
            .body(stream)
            .part_number(part_number)
            .send()
            .await {
                Ok(result) => result,
                Err(err) => panic!("Failed to upload part: {err:?}")
            };

        let etag = match upload_part_res.e_tag {
            Some(etag) => etag,
            None => panic!("Failed to get etag for part: {part_number}")
        };

        self.upload_parts.push(
                CompletedPart::builder()
                    .e_tag(etag)
                    .part_number(part_number)
                    .build());

        Ok(())
    }


    async fn close(&mut self) -> IOResult<()> {
        let upload_parts = mem::take(&mut self.upload_parts);
        let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();
        let upload_id = self.upload_id.as_ref().unwrap_or_else(|| panic!("No upload-id set, so the multi-part upload has not been started.")).clone();

        match self.client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(&self.object_name)
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await {
                Ok(res) => {
                        println!("Multipart Upload {}:{} of {} bytes finished succesfully", self.bucket_name, self.object_name, self.length);
                        println!("With results {res:?}");
                        self.closed = true;
                    }
                Err(err) => {
                        eprintln!("On Close: Multipart Upload {}:{} failed with error:\n{err:?}\n", self.bucket_name, self.object_name);
                        return Err(io::Error::new(io::ErrorKind::Other, format!("AWS-sdk error: {err:?}")));
                    }
            }

        Ok(())
    }
    
}