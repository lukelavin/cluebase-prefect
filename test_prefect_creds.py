import boto3
from prefect_aws import AwsCredentials, S3Bucket

aws_credentials_block = AwsCredentials.load("cluebase-credentials")
s3_bucket = S3Bucket(bucket_name="cluebase", credentials=aws_credentials_block)
response = s3_bucket.list_objects("raw/seasons/")

s3_client = s3_bucket._get_s3_client()
for bucket in response:
    print(bucket["Key"])
