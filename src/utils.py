import os

import requests
from pymongo import AsyncMongoClient

client = AsyncMongoClient("mongodb://localhost:27017/")
db = client.cluebase


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


async def insert_clue(clue_dict):
    result = await db.clues.insert_one(clue_dict)
    return result.inserted_id


async def insert_clue_bulk(clue_list):
    result = await db.clues.insert_many(clue_list)
    return result.inserted_ids
