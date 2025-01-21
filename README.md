# s3-tee

A command-line utility that streams stdin to both stdout and Amazon S3, similar to the Unix `tee` command but with S3 as an additional output.

## Features

- Stream data simultaneously to stdout and S3
- Support for AWS profiles and regions
- Automatic handling of large files
- Resumable uploads
- Buffering and streaming

## Warning

To allow real-time outputs, this tool will produce **one PutObject request at every second**, which will be charged every time.

## Installation

```boto3
cargo install s3-tee
```

## Usage

my-bucket have to be a directory bucket.

```boto3
s3-tee --bucket my-bucket --key my-key
```


### Arguments

- `--bucket`: S3 bucket name (required)
- `--key`: S3 object key/path (required)
- `--profile`: AWS profile name (optional, defaults to "default")
- `--region`: AWS region (optional, uses default from AWS configuration)

### Example
```bash
# Stream a file to both stdout and S3
cat large-file.txt | s3-tee --bucket my-bucket --key logs/large-file.txt

# Use with other Unix commands
echo "Hello, World!" | s3-tee --bucket my-bucket --key hello.txt

# Specify AWS profile and region
python3 train.py | s3-tee --bucket logs-bucket --key app.log --profile prod --region us-west-2
```


## AWS Configuration

The tool uses the AWS SDK for Rust and follows standard AWS configuration practices. Make sure you have:

1. AWS credentials configured (`~/.aws/credentials`)
2. AWS configuration set (`~/.aws/config`)

# License
MIT