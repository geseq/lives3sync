lives3syncd
-----------

Continually sync a local filesystem to S3.

This functions just like rsync and will monitor for new files and immediately sync them to S3. Multiple uploads to S3 are handled concurrently, and large files are chunked.


```
Usage of ./lives3syncd:
  -bucket string
    	S3 bucket name
  -dry-run
    	dry run only - don't upload files
  -exclude value
    	pattern to exclude (may be given multiple times. Multiple patterns OR'd together)
  -match value
    	pattern to match (may be given multiple times. Multiple patterns OR'd together)
  -parallel int
    	parallel uploads (defaults to number of available cores)
  -prefix string
    	prefix for content in s3
  -region string
    	AWS S3 Region (default "us-east-1")
  -run-once
    	exit after syncing existing files (ie: don't wait for updates)
  -src string
    	source directory to sync
```

## Configuring S3 Credentials

Before using, ensure that you've configured credentials appropriately. The best way to configure credentials is to use the `~/.aws/credentials` file, which might look like:

```
[default]
aws_access_key_id = AKID1234567890
aws_secret_access_key = MY-SECRET-KEY
```

You can learn more about the credentials file from [this blog post](http://blogs.aws.amazon.com/security/post/Tx3D6U6WSFGOK2H/A-New-and-Standardized-Way-to-Manage-Credentials-in-the-AWS-SDKs).

Alternatively, you can set the following environment variables:

```
AWS_ACCESS_KEY_ID=AKID1234567890
AWS_SECRET_ACCESS_KEY=MY-SECRET-KEY
```

## Installing

#### Building From Source

This project uses [gb](https://getgb.io/) the Go Build tool. To load vendored dependencies and compile run `./vendor.sh && gb build`