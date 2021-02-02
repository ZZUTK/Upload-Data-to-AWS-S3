# Upload Data to AWS S3
Upload local file or folder to S3, and download S3 file or folder to local.

- [Get Credentials](#get-credentials)
- [Upload to Bucket](#upload-data-to-bucket)
- [Download from Bucket](#download-data-from-bucket)
- [List Bucket Files](#list-files-in-bucket)
- [Delete Bucket](#delete-files-from-bucket)
- [An Quick Example](#an-quick-example)

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

## Download Data from Bucket
```bash
python upload_to_s3.py <bucket_name> download --data <prefix/to/bucket/files> --download-dir <local_dir_to_save_data>
```
If not set `--data`, it will download all files in the bucket. 
If not set `--download-dir`, it will download to local folder `./s3_downloads`.

## List Files in Bucket

```bash
python upload_to_s3.py <bucket_name> list
```

## Delete Files from Bucket

```bash
python upload_to_s3.py <bucket_name> delete --data <prefix/to/bucket/files>
```

If not set `--data`, it will delete the whole bucket.


## An Quick Example

```bash
python upload_to_s3.py test-upload-data-to-aws-s3 upload --data ./

Upload Start
[100/101] ETA: 00 sec
29 files left ...
Done: 101 file(s) -> Bucket test-upload-data-to-aws-s3
```
```bash
python upload_to_s3.py test-upload-data-to-aws-s3 list

...
README.md
credentials
file_list.txt
meta.txt
upload_to_s3.py
...
```

```bash
python upload_to_s3.py test-upload-data-to-aws-s3 download

Find 101 files in Bucket test-upload-data-to-aws-s3
Download all files?[y/n]y
...
Downloading Bucket test-upload-data-to-aws-s3/README.md -> ./s3_downloads/README.md
Downloading Bucket test-upload-data-to-aws-s3/credentials -> ./s3_downloads/credentials
Downloading Bucket test-upload-data-to-aws-s3/file_list.txt -> ./s3_downloads/file_list.txt
Downloading Bucket test-upload-data-to-aws-s3/meta.txt -> ./s3_downloads/meta.txt
Downloading Bucket test-upload-data-to-aws-s3/upload_to_s3.py -> ./s3_downloads/upload_to_s3.py
...
```

```bash
python upload_to_s3.py test-upload-data-to-aws-s3 delete

Delete the whole Bucket test-upload-data-to-aws-s3?[y/n]y
...
Deleted README.md
Deleted credentials
Deleted file_list.txt
Deleted meta.txt
Deleted upload_to_s3.py
Deleted 101 files, remain 0 files in Bucket test-upload-data-to-aws-s3
Deleted Bucket test-upload-data-to-aws-s3
```






