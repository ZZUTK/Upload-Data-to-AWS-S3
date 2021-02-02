# Upload Data to AWS S3
Upload local file or folder to S3, and download S3 file or folder to local.

## Pre-requisites
* Python 3.x
* [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html?id=docs_gateway)

Tested on Ubuntu 16.04 and Mac 10.15.7

## Get Credentials
Follow the [instruction](https://wiki.corp.adobe.com/pages/viewpage.action?spaceKey=~bwai&title=How+to+use+KLAM+to+authenticate+into+an+AWS+account+for+Adobe+Research)
to `Copy Credentials` and paste to [`./credentials`](./credentials)

## Upload Data to Bucket

```bash
python upload_to_s3.py <bucket_name> upload --data <path/to/local/file_or_folder>
```

Upload data to a specific folder in the bucket:
```bash
python upload_to_s3.py <bucket_name> upload --data <path/to/local/file_or_folder> --folder <bucket_folder>
```

If you want to make the data public on S3:

```bash
python upload_to_s3.py <bucket_name> upload --data <path/to/local/file_or_folder> --public
```

## List Files in Bucket

```bash
python upload_to_s3.py <bucket_name> list
```

## Download Data from Bucket
```bash
python upload_to_s3.py <bucket_name> download --data <prefix/to/bucket/files> --download-dir <local_dir_to_save_data>
```
If not set `--data`, it will download all files in the bucket. 
If not set `--download-dir`, it will download to local folder `./s3_downloads`.


## Delete Files from Bucket

```bash
python upload_to_s3.py <bucket_name> delete --data <prefix/to/bucket/files>
```

If not set `--data`, it will delete the whole bucket.




