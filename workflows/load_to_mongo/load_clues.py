import asyncio
import logging
import os
from logging import getLogger

import pymongo
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect_aws.s3 import S3Bucket
from prefect.cache_policies import INPUTS
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
async def load_clues_from_game_file_s3(bucket: S3Bucket, s3_path: str, db: Database):
    game_html = await read_s3_object_async(bucket, s3_path)

    game_id = s3_path.split("/")[-1].split(".")[0]
    file_logger.debug(f"Loading clues from game: {game_id}")

    clues = parse_clues_from_game(game_html, game_id)

    file_logger.debug(f"Attempting to load {len(clues)} clues into collection")
    return await insert_clue_bulk(db, clues)


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

    with tqdm(games) as progress:
        async for game_file in progress:
            try:
                future = load_clues_from_game_file_s3.submit(bucket, game_file, db)
                loaded = future.result()
                file_logger.debug(f"Loaded {loaded} clues from {game_file}")
            except pymongo.errors.BulkWriteError as e:
                file_logger.warn(e)


@flow
async def load_clues_from_all_games_s3(
    s3_bucket_name="cluebase", s3_games_path="raw/games"
)
    load_all_game_files_s3(s3_bucket_name, s3_games_path)


# if __name__ == "__main__":

#     # asyncio.run(load_clues_from_game_file("6737.html"))

#     asyncio.run(load_all_game_files())
