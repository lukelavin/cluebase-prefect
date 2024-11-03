import asyncio
import itertools
import os
from logging import getLogger

import pymongo
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.logging import get_logger, get_run_logger

from src.clues import parse_clues_from_game
from src.io_utils import (
    get_mongo_client,
    get_s3_bucket,
    insert_clue_bulk,
    ls_s3_prefix,
    read_s3_object_async,
)
from src.paths import RAW_GAMES_DIR

file_logger = getLogger(__name__)


async def read_and_parse_clues(bucket, s3_path, logger=get_logger()):
    game_html = await read_s3_object_async(bucket, s3_path)

    game_id = s3_path.split("/")[-1].split(".")[0]
    logger.debug(f"Loading clues from game: {game_id}")

    return parse_clues_from_game(game_html, game_id)


@task
async def load_clues_batch_s3(
    s3_bucket_name: str = "cluebase",
    games_dir: str = RAW_GAMES_DIR,
    game_file_prefix: str = "",
    mongo_secret_block: str = "mongo-connection-string",
    database_name: str = "cluebase",
):
    logger = get_run_logger()
    logger.info(
        f"Loading clues from s3:/{s3_bucket_name}/{games_dir}/{game_file_prefix}*"
    )

    bucket = get_s3_bucket(s3_bucket_name)
    game_paths = await ls_s3_prefix(bucket, games_dir, prefix=game_file_prefix)
    logger.info(f"Found {len(game_paths)} games to load")

    clue_lists = await asyncio.gather(
        *[read_and_parse_clues(bucket, s3_path, logger) for s3_path in game_paths]
    )

    clues = list(itertools.chain.from_iterable(clue_lists))
    logger.info(f"Clues parsed: {len(clues)}")

    logger.info(f"Getting Mongo connection using secret block {mongo_secret_block}")
    mongo_conn_str = (await Secret.load(mongo_secret_block)).get()
    mongo_client = await get_mongo_client(mongo_conn_str)
    db = mongo_client.get_database(database_name)

    logger.info(f"Attempting to load clues into collection")
    try:
        loaded = await insert_clue_bulk(db, clues)
        logger.info(
            f"Loaded {len(loaded)} clues from s3:/{s3_bucket_name}/{games_dir}/{game_file_prefix}*"
        )
        logger.info(loaded)
        return loaded
    except pymongo.errors.BulkWriteError as e:
        logger.warning(e)
        return []


@task
async def load_all_game_files_batched_s3(
    s3_bucket_name: str = "cluebase",
    games_dir: str = RAW_GAMES_DIR,
    mongo_secret_block: str = "mongo-connection-string",
):
    logger = get_run_logger()
    logger.info("Loading clues batched from all game files")

    batch_runs = []
    for batch_prefix in range(1, 10):
        batch_runs.append(
            load_clues_batch_s3.submit(
                s3_bucket_name, games_dir, str(batch_prefix), mongo_secret_block
            )
        )

    results = [run.result() for run in batch_runs]


@flow
def load_clues_from_all_games_s3(s3_bucket_name="cluebase", s3_games_path="raw/games"):
    asyncio.run(load_all_game_files_batched_s3(s3_bucket_name, s3_games_path))
