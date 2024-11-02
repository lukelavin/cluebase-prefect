import asyncio
import itertools
import logging
import os
from logging import getLogger

import pymongo
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.cache_policies import INPUTS
from prefect_aws.s3 import S3Bucket
from pymongo.database import Database
from tqdm.asyncio import tqdm

from src.clues import parse_clues_from_game
from src.io_utils import (
    get_db,
    get_mongo_client,
    get_s3_bucket,
    insert_clue_bulk,
    ls_s3,
    ls_s3_async,
    ls_s3_prefix,
    read_raw_file,
    read_s3_object_async,
)
from src.paths import RAW_GAMES_DIR

file_logger = logging.getLogger(__name__)


@task(cache_policy=INPUTS - "db")
async def load_clues_from_game_file(game_file: str, db: Database):
    game_html = read_raw_file(os.path.join(RAW_GAMES_DIR, game_file))

    game_id = game_file.split(".")[0]
    file_logger.debug(f"Loading clues from game: {game_id}")

    clues = parse_clues_from_game(game_html, game_id)

    await insert_clue_bulk(db, clues)


@task(cache_policy=INPUTS - "db")
async def load_clues_from_game_file_s3_task(
    bucket: S3Bucket, s3_path: str, db: Database
):
    return load_clues_from_game_file_s3(bucket, s3_path, db)


async def load_clues_from_game_file_s3(bucket: S3Bucket, s3_path: str, db: Database):
    game_html = await read_s3_object_async(bucket, s3_path)

    game_id = s3_path.split("/")[-1].split(".")[0]
    file_logger.debug(f"Loading clues from game: {game_id}")

    clues = parse_clues_from_game(game_html, game_id)

    file_logger.debug(f"Attempting to load {len(clues)} clues into collection")
    try:
        loaded = await insert_clue_bulk(db, clues)
        file_logger.debug(f"Loaded {loaded} clues from {s3_path}")
        return loaded
    except pymongo.errors.BulkWriteError as e:
        file_logger.warning(e)
        return []


async def read_and_parse_clues(bucket, s3_path):
    game_html = await read_s3_object_async(bucket, s3_path)

    game_id = s3_path.split("/")[-1].split(".")[0]
    file_logger.debug(f"Loading clues from game: {game_id}")

    return parse_clues_from_game(game_html, game_id)


@task(cache_policy=INPUTS - "db")
async def load_clues_batch_s3(
    s3_bucket_name: str = "cluebase",
    games_dir: str = RAW_GAMES_DIR,
    game_file_prefix: str = "",
    mongo_secret_block: str = "mongo-connection-string",
):
    file_logger.info("Loading clues from all game files")

    bucket = get_s3_bucket(s3_bucket_name)

    mongo_conn_str = (await Secret.load(mongo_secret_block)).get()
    mongo_client = await get_mongo_client(mongo_conn_str)
    db = await get_db(mongo_client)

    game_paths = await ls_s3_prefix(bucket, games_dir, prefix=game_file_prefix)

    clue_lists = [
        await asyncio.gather(
            [read_and_parse_clues(bucket, s3_path) for s3_path in game_paths]
        )
    ]

    clues = list(itertools.chain_from_iterable(clue_lists))

    file_logger.debug(f"Attempting to load {len(clues)} clues into collection")
    try:
        loaded = await insert_clue_bulk(db, clues)
        file_logger.debug(
            f"Loaded {loaded} clues from s3:/{s3_bucket_name}/{games_dir}/{game_file_prefix}*"
        )
        return loaded
    except pymongo.errors.BulkWriteError as e:
        file_logger.warning(e)
        return []


@task
async def load_all_game_files(games_dir=RAW_GAMES_DIR):
    with tqdm(os.listdir(games_dir)) as progress:
        async for game_file in progress:
            try:
                await load_clues_from_game_file(game_file)
            except pymongo.errors.BulkWriteError as e:
                file_logger.warning(e)


@task
async def load_all_game_files_s3(
    s3_bucket_name: str = "cluebase",
    games_dir: str = RAW_GAMES_DIR,
    mongo_secret_block: str = "mongo-connection-string",
):
    file_logger.info("Loading clues from all game files")

    bucket = get_s3_bucket(s3_bucket_name)

    mongo_conn_str = (await Secret.load(mongo_secret_block)).get()
    mongo_client = await get_mongo_client(mongo_conn_str)
    db = await get_db(mongo_client)

    games = await ls_s3_async(s3_bucket_name, games_dir)
    file_logger.info(f"Found {len(games)} games to load")
    file_logger.info(f"Game examples: {games[:5]}")

    futures = []
    with tqdm(games) as progress:
        async for game_file in progress:
            futures.append(load_clues_from_game_file_s3.submit(bucket, game_file, db))

    results = [future.result() for future in futures]


@task
async def load_all_game_files_batched_s3(
    s3_bucket_name: str = "cluebase",
    games_dir: str = RAW_GAMES_DIR,
    mongo_secret_block: str = "mongo-connection-string",
):
    file_logger.info("Loading clues batched from all game files")

    batch_runs = []
    for batch_prefix in range(1, 10):
        file_logger.info("Loading file batch prefix: " + str(batch_prefix))
        batch_runs.append(
            load_clues_batch_s3.submit(
                s3_bucket_name, games_dir, str(batch_prefix), mongo_secret_block
            )
        )

    results = [run.result() for run in batch_runs]


@flow
def load_clues_from_all_games_s3(s3_bucket_name="cluebase", s3_games_path="raw/games"):
    asyncio.run(load_all_game_files_batched_s3(s3_bucket_name, s3_games_path))


# if __name__ == "__main__":

#     # asyncio.run(load_clues_from_game_file("6737.html"))

#     asyncio.run(load_all_game_files())
