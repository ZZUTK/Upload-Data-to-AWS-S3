# Upload Data to AWS S3

## Pre-requisites
* Python 3.x
* [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html?id=docs_gateway)

Tested on Ubuntu 16.04

## Get Credentials
Go to https://klam-sj.corp.adobe.com/aws/adobeaws.php?userData=klam

## Upload Data

```
$ python upload_to_s3.py --bucket <bucket_name> --data_dir <path/to/data/folder> 
  
```

If you want to make the data public on S3:

```
$ python upload_to_s3.py --bucket <bucket_name> --data_dir <path/to/data/folder> --public
  
```

## Delete Bucket

```
$ python upload_to_s3.py --bucket <bucket_name> --action delete
  
```

## Check Uploaded Files

```
$ python upload_to_s3.py --bucket <bucket_name> --action check
  
```
