import asyncio
import os
from functools import cache, partial
from io import BytesIO
from logging import getLogger
from typing import Any, Dict, List, Optional

import requests
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from prefect_aws import AwsCredentials, S3Bucket
from pymongo import AsyncMongoClient, MongoClient
from pymongo.database import Database

file_logger = getLogger(__name__)


def download_html(url, write_path, overwrite=False, logger=file_logger):
    if os.path.exists(write_path) and not overwrite:
        logger.debug(f"{write_path} exists, skipping download")
        return False

    r = requests.get(url)

    if r.ok:
        logger.info(f"Writing to {write_path}")
        with open(write_path, "w+") as f:
            f.write(r.text)

        return True
    else:
        logger.error("Error {r.status_code} downloading page {url}")
        logger.error(r)
        return False


def read_raw_file(file_path, logger=file_logger):
    with open(file_path, "r") as f:
        return f.read()


def print_to_file(content, file_path="test.html", logger=file_logger):
    with open(file_path, "w+") as f:
        f.write(str(content))


# *****************************************************
# Mongo helpers
# *****************************************************


async def get_mongo_client(connection_string: str = "mongodb://localhost:27017/"):
    return AsyncMongoClient(connection_string)


async def get_db(client: AsyncMongoClient, db_name: str = "cluebase") -> Database:
    return client.get_database(db_name)


async def insert_clue(db, clue_dict):
    result = await db.clues.insert_one(clue_dict)
    return result.inserted_id


async def insert_clue_bulk(db, clue_list):
    result = await db.clues.insert_many(clue_list, ordered=False)
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
    return path in ls_s3(bucket.bucket_name, path[: path.rfind("/")])
    # return bool(bucket.list_objects(path))


def upload_object(bucket: S3Bucket, path: str, content: str) -> str:
    bytes_content = BytesIO(bytes(content, "utf-8"))
    result_path = bucket.upload_from_file_object(bytes_content, path)
    return result_path


def download_html_to_s3(
    url: str, bucket: S3Bucket, write_path: str, overwrite=False, logger=file_logger
) -> str:
    if object_exists(bucket, write_path) and not overwrite:
        logger.debug(f"{write_path} exists, skipping download")
        return None

    r = requests.get(url)

    if r.ok:
        logger.info(f"Writing to {write_path}")
        path = upload_object(bucket, write_path, r.text)

        return path
    else:
        logger.error("Error {r.status_code} downloading page {url}")
        logger.error(r)
        return None


def read_s3_object(bucket: S3Bucket, path: str) -> str:
    return bucket.read_path(path)


async def read_s3_object_async(bucket: S3Bucket, path: str) -> str:
    loop = asyncio.get_event_loop()

    read_object_partial = partial(bucket.read_path, path)

    return await loop.run_in_executor(None, read_object_partial)


@cache
def ls_s3(bucket_name: str, path: str) -> str:
    bucket = get_s3_bucket(bucket_name)
    response = bucket.list_objects(path)

    return tuple(s3_object["Key"] for s3_object in response)


async def ls_s3_prefix(
    bucket: S3Bucket,
    folder: str = "",
    delimiter: str = "",
    prefix: str = "",
    page_size: Optional[int] = None,
    max_items: Optional[int] = None,
    jmespath_query: Optional[str] = None,
    logger=file_logger,
) -> List[Dict[str, Any]]:
    bucket_path = bucket._join_bucket_folder(folder) + "/" + prefix
    client = bucket.credentials.get_s3_client()
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=bucket.bucket_name,
        Prefix=bucket_path,
        Delimiter=delimiter,
        PaginationConfig={"PageSize": page_size, "MaxItems": max_items},
    )
    if jmespath_query:
        page_iterator = page_iterator.search(f"{jmespath_query} | {{Contents: @}}")

    logger.info(f"Listing objects in bucket {bucket_path}.")
    objects = await run_sync_in_worker_thread(bucket._list_objects_sync, page_iterator)

    return tuple(s3_object["Key"] for s3_object in objects)
