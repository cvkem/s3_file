use std::{
    io,
    thread
};
use bytes::{Bytes};
use tokio;
use crate::object_writer::{ObjectWriter, ObjectWriterAux};


pub struct WriteSink {
    handle: Option<thread::JoinHandle<()>>,
    send_channel: Option<tokio::sync::mpsc::Sender<Bytes>>
}

impl WriteSink {

    pub fn new(bucket_name: String, object_name: String) -> Self {

        let (send_channel, mut receiver_channel) = tokio::sync::mpsc::channel(2);

        // Build the runtime for the new thread.
        //
        // The runtime is created before spawning the thread
        // to more cleanly forward errors if the `unwrap()`
        // panics.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let handle = thread::spawn(move || {
            let mut obj_writer = ObjectWriter::new(bucket_name, object_name);
            if let Err(err) =  rt.block_on(obj_writer.create_multipart_upload()) {
                eprintln!("Error while creating multipart upload: {err:?}");
            };
            println!(" Created multi-part upload. Now start loop.");

            // on a this thread start receiving chunks of Bytes.
            let mut receive_counter = 0;
            rt.block_on(async move {
                while let Some(bs) = receiver_channel.recv().await {
                    receive_counter += 1;
                    println!(" Received block {receive_counter}. Now writing to s3-storage.");
                    if let Err(err) = obj_writer.upload_part(bs).await {
                        eprintln!("Error while writing chunck {receive_counter}: {err:?}");
                    };
                }
                if let Err(err) = obj_writer.close().await {
                    eprintln!("Error while closing writer: {err:?}");
                };
            });
        });        
        let handle = Some(handle);
        let send_channel = Some(send_channel);
        Self {handle, send_channel}
    }

    pub fn send_bytes(&self, bytes: Bytes) {
        match self.send_channel.as_ref().unwrap().blocking_send(bytes) {
            Ok(()) => {},
            Err(_) => panic!("The shared runtime has shut down."),
        }
    }

    pub fn close(&mut self) -> io::Result<()> {
        drop(self.send_channel.take());
        if let Err(err) = self.handle
            .take()
            .expect("join-handle has been resolved already")
            .join() {
                eprintln!("Error while joining handle:  {err:?}");
        };
        Ok(())
    }

}