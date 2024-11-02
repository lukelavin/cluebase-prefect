import os
from functools import cache
from io import BytesIO

import requests
from prefect_aws import AwsCredentials, S3Bucket
from pymongo import AsyncMongoClient


def download_html(url, write_path, overwrite=False):
    if os.path.exists(write_path) and not overwrite:
        print(f"{write_path} exists, skipping download")
        return False

    r = requests.get(url)

    if r.ok:
        print(f"Writing to {write_path}")
        with open(write_path, "w+") as f:
            f.write(r.text)

        return True
    else:
        print("Error {r.status_code} downloading page {url}")
        print(r)
        return False


def read_raw_file(file_path):
    with open(file_path, "r") as f:
        return f.read()


def print_to_file(content, file_path="test.html"):
    with open(file_path, "w+") as f:
        f.write(str(content))


# *****************************************************
# Mongo helpers
# *****************************************************

client = AsyncMongoClient("mongodb://localhost:27017/")
db = client.cluebase


async def insert_clue(clue_dict):
    result = await db.clues.insert_one(clue_dict)
    return result.inserted_id


async def insert_clue_bulk(clue_list):
    result = await db.clues.insert_many(clue_list)
    return result.inserted_ids


# *****************************************************
# Prefect S3 helpers
# *****************************************************

aws_creds = AwsCredentials.load("cluebase-credentials")


def get_s3_bucket(
    bucket_name: str, credentials: AwsCredentials = aws_creds
) -> S3Bucket:
    return S3Bucket(bucket_name=bucket_name, credentials=credentials)


def object_exists(bucket: S3Bucket, path: str) -> bool:
    return path in ls_s3(bucket, path.split("/")[:-1])
    # return bool(bucket.list_objects(path))


def upload_object(bucket: S3Bucket, path: str, content: str) -> str:
    bytes_content = BytesIO(bytes(content, "utf-8"))
    result_path = bucket.upload_from_file_object(bytes_content, path)
    return result_path


def download_html_to_s3(
    url: str, bucket: S3Bucket, write_path: str, overwrite=False
) -> str:
    if object_exists(bucket, write_path) and not overwrite:
        print(f"{write_path} exists, skipping download")
        return None

    r = requests.get(url)

    if r.ok:
        print(f"Writing to {write_path}")
        path = upload_object(bucket, write_path, r.text)

        return path
    else:
        print("Error {r.status_code} downloading page {url}")
        print(r)
        return None


def read_s3_object(bucket: S3Bucket, path: str) -> str:
    return bucket.read_path(path)


@cache()
def ls_s3(bucket: S3Bucket, path: str) -> str:
    response = bucket.list_objects(path)

    return [s3_object["Key"] for s3_object in response]
