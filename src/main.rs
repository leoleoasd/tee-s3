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
use tokio::time::{self, Duration};

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

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    // Initialize AWS S3 client with profile and region
    let mut config_builder = aws_config::defaults(BehaviorVersion::latest());

    // Set profile if provided
    if let Some(profile) = args.profile {
        config_builder = config_builder.profile_name(profile);
    }

    // Set region if provided
    if let Some(region) = args.region {
        config_builder = config_builder.region(Region::new(region));
    }

    let config = config_builder.load().await;
    let client = Client::new(&config);

    // Create a shared BytesMut buffer
    let shared_buffer = Arc::new(Mutex::new(BytesMut::new()));

    // Clone the buffer for the producer (stdin task)
    let producer_buffer = Arc::clone(&shared_buffer);

    // Create a shared flag to indicate when stdin is done
    let stdin_done = Arc::new(Mutex::new(false));
    let stdin_done_producer = Arc::clone(&stdin_done);
    let stdin_done_consumer = Arc::clone(&stdin_done);

    // Task for reading stdin
    let stdin_task = tokio::spawn(async move {
        let mut stdin = io::stdin();
        let mut buffer = [0; 1024];

        loop {
            match stdin.read(&mut buffer).await {
                Ok(0) => {
                    // Set the done flag when EOF is reached
                    *stdin_done_producer.lock().await = true;
                    return Ok(());
                }
                Ok(bytes_read) => {
                    let mut shared = producer_buffer.lock().await;
                    io::stdout().write_all(&buffer[..bytes_read]).await?;
                    io::stdout().flush().await?;
                    shared.extend_from_slice(&buffer[..bytes_read]);
                }
                Err(e) => {
                    eprintln!("Error reading from stdin: {}", e);
                    return Err(e);
                }
            }
        }
    });

    // Clone the buffer for the consumer (S3 task)
    let consumer_buffer = Arc::clone(&shared_buffer);
    let s3_client = client.clone();

    // Task for sending data to S3
    let output_task = tokio::spawn(async move {
        let mut bytes_written;
        // read object size
        let object = s3_client
            .head_object()
            .bucket(&args.bucket)
            .key(&args.key)
            .send()
            .await;
        if let Err(e) = object {
            // object not found, start from the beginning
            bytes_written = 0;
        } else {
            // object found, start from the end
            bytes_written = object.unwrap().content_length().unwrap_or(0);
        }
        let mut parts = 0;
        loop {
            let mut shared = consumer_buffer.lock().await;
            if shared.is_empty() {
                // Check if stdin is done and buffer is empty
                if *stdin_done_consumer.lock().await {
                    return Ok(());
                }
                drop(shared);
                time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            let data = shared.split_to(1);
            let data: Bytes = data.freeze();
            let data_len = data.len();
            drop(shared);

            // Send data to S3
            let mut request = s3_client
                .put_object()
                .bucket(&args.bucket)
                .key(&args.key)
                .set_checksum_algorithm(Some(ChecksumAlgorithm::Crc32C));

            if bytes_written > 0 {
                request = request.set_write_offset_bytes(Some(bytes_written));
            }

            match request.body(data.clone().into()).send().await {
                Ok(_) => {
                    bytes_written += data_len as i64;
                }
                Err(e) => {
                    if let SdkError::ServiceError(ref err) = e {
                        if matches!(err.err(), PutObjectError::TooManyParts(_)) {
                            // copy to a new object, copy back and retry
                            let new_key = format!("{}.1", args.key);
                            let new_object = s3_client
                                .copy_object()
                                .bucket(&args.bucket)
                                .key(&new_key)
                                .copy_source(format!("{}/{}", args.bucket, args.key))
                                .send()
                                .await;
                            if let Err(e) = new_object {
                                eprintln!("Failed to copy to new object: {:?}", e);
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 upload error: {:?}", e),
                                ));
                            }
                            // copy back to original object
                            let copy_back = s3_client
                                .copy_object()
                                .bucket(&args.bucket)
                                .key(&args.key)
                                .copy_source(format!("{}/{}", args.bucket, new_key))
                                .send()
                                .await;
                            if let Err(e) = copy_back {
                                eprintln!("Failed to copy back to original object: {:?}", e);
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 upload error: {:?}", e),
                                ));
                            }
                            // delete the new object
                            let delete_new = s3_client
                                .delete_object()
                                .bucket(&args.bucket)
                                .key(&new_key)
                                .send()
                                .await;
                            if let Err(e) = delete_new {
                                eprintln!("Failed to delete new object: {:?}", e);
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 upload error: {:?}", e),
                                ));
                            }
                            // retry
                            let request = s3_client
                                .put_object()
                                .bucket(&args.bucket)
                                .key(&args.key)
                                .set_checksum_algorithm(Some(ChecksumAlgorithm::Crc32C))
                                .set_write_offset_bytes(Some(bytes_written));
                            match request.body(data.into()).send().await {
                                Ok(_) => {
                                    bytes_written += data_len as i64;
                                }
                                Err(e) => {
                                    eprintln!("Failed to retry upload: {:?}", e);
                                    return Err(io::Error::new(
                                        io::ErrorKind::Other,
                                        format!("S3 upload error: {:?}", e),
                                    ));
                                }
                            }
                            continue;
                        }
                    }
                    eprintln!("Failed to upload to S3: {:?}", e);
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("S3 upload error: {:?}", e),
                    ));
                }
            }
            parts += 1;
            println!("parts: {}", parts);
        }
    });

    // Run tasks and handle their results
    match tokio::try_join!(stdin_task, output_task) {
        Ok((stdin_result, output_result)) => {
            stdin_result?;
            output_result?;
            Ok(())
        }
        Err(e) => {
            eprintln!("Task error: {}", e);
            Err(io::Error::new(io::ErrorKind::Other, "Task failed"))
        }
    }
}
