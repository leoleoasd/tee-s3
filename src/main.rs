use aws_config::BehaviorVersion;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::types::ChecksumAlgorithm;
use aws_sdk_s3::{config::Region, Client};
use bytes::{Bytes, BytesMut};
use clap::Parser;
use std::sync::Arc;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

const BUFFER_SIZE: usize = 8192; // 8KB buffer size

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Tee to S3 - streams stdin to both stdout and S3"
)]
struct Args {
    /// AWS Profile name (optional, uses default if not specified)
    #[arg(long)]
    profile: Option<String>,

    /// AWS Region (optional)
    #[arg(long)]
    region: Option<String>,

    /// S3 bucket name
    #[arg(long)]
    bucket: String,

    /// S3 object key
    #[arg(long)]
    key: String,
}

struct S3Uploader {
    client: Client,
    bucket: String,
    key: String,
    bytes_written: i64,
}

impl S3Uploader {
    fn new(client: Client, bucket: String, key: String) -> Self {
        Self {
            client,
            bucket,
            key,
            bytes_written: 0,
        }
    }

    async fn initialize(&mut self) -> io::Result<()> {
        // Check if object exists and get its size
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .send()
            .await
        {
            Ok(response) => {
                self.bytes_written = response.content_length().unwrap_or(0);
            }
            Err(_) => {
                self.bytes_written = 0; // Start from beginning for new objects
            }
        }
        Ok(())
    }

    async fn upload_chunk(&mut self, data: Bytes) -> io::Result<()> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .set_checksum_algorithm(Some(ChecksumAlgorithm::Crc32C));

        if self.bytes_written > 0 {
            request = request.set_write_offset_bytes(Some(self.bytes_written));
        }

        match request.body(data.clone().into()).send().await {
            Ok(_) => {
                self.bytes_written += data.len() as i64;
                Ok(())
            }
            Err(e) => {
                if let SdkError::ServiceError(err) = &e {
                    if matches!(err.err(), PutObjectError::TooManyParts(_)) {
                        self.handle_too_many_parts(data).await?;
                        return Ok(());
                    }
                }
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("S3 upload error: {:?}", e),
                ))
            }
        }
    }

    async fn handle_too_many_parts(&mut self, data: Bytes) -> io::Result<()> {
        let temp_key = format!("{}.temp", self.key);

        // Copy to temporary object
        self.copy_object(&self.key, &temp_key).await?;

        // Copy back to original
        self.copy_object(&temp_key, &self.key).await?;

        // Delete temporary object
        self.delete_object(&temp_key).await?;

        // Retry the upload
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .set_checksum_algorithm(Some(ChecksumAlgorithm::Crc32C))
            .set_write_offset_bytes(Some(self.bytes_written));

        match request.body(data.clone().into()).send().await {
            Ok(_) => {
                self.bytes_written += data.len() as i64;
                Ok(())
            }
            Err(e) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to retry upload: {:?}", e),
            )),
        }
    }

    async fn copy_object(&self, from_key: &str, to_key: &str) -> io::Result<()> {
        self.client
            .copy_object()
            .bucket(&self.bucket)
            .key(to_key)
            .copy_source(format!("{}/{}", self.bucket, from_key))
            .send()
            .await
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to copy object: {:?}", e),
                )
            })?;
        Ok(())
    }

    async fn delete_object(&self, key: &str) -> io::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to delete object: {:?}", e),
                )
            })?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    // Initialize AWS configuration
    let config = aws_config::defaults(BehaviorVersion::latest())
        .profile_name(args.profile.unwrap_or("default".to_string()))
        .region(args.region.map(Region::new))
        .load()
        .await;

    let client = Client::new(&config);
    let mut uploader = S3Uploader::new(client, args.bucket, args.key);
    uploader.initialize().await?;

    let buffer = Arc::new(Mutex::new(BytesMut::with_capacity(BUFFER_SIZE)));
    let is_done = Arc::new(Mutex::new(false));

    // Spawn stdin reader task
    let stdin_buffer = Arc::clone(&buffer);
    let stdin_done = Arc::clone(&is_done);
    let stdin_handle = tokio::spawn(async move {
        let mut stdin = io::stdin();
        let mut read_buffer = [0; BUFFER_SIZE];

        loop {
            match stdin.read(&mut read_buffer).await {
                Ok(0) => {
                    *stdin_done.lock().await = true;
                    break Ok(()) as io::Result<()>;
                }
                Ok(n) => {
                    io::stdout().write_all(&read_buffer[..n]).await?;
                    io::stdout().flush().await?;
                    stdin_buffer
                        .lock()
                        .await
                        .extend_from_slice(&read_buffer[..n]);
                }
                Err(e) => break Err(e),
            }
        }
    });

    // Spawn S3 uploader task
    let upload_buffer = Arc::clone(&buffer);
    let upload_done = Arc::clone(&is_done);
    let upload_handle: tokio::task::JoinHandle<io::Result<()>> = tokio::spawn(async move {
        while !*upload_done.lock().await || !upload_buffer.lock().await.is_empty() {
            let chunk = {
                let mut buffer = upload_buffer.lock().await;
                if buffer.is_empty() {
                    continue;
                }
                buffer.split().freeze()
            };

            uploader.upload_chunk(chunk).await?;
        }
        Ok(())
    });

    // Wait for both tasks to complete
    match tokio::try_join!(stdin_handle, upload_handle) {
        Ok((stdin_result, upload_result)) => {
            stdin_result?;
            upload_result?;
            Ok(())
        }
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
    }
}
