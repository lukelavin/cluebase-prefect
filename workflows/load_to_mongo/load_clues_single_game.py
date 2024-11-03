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
async def load_game_file_s3(
    game_id: str,
    s3_bucket_name: str = "cluebase",
    games_dir: str = RAW_GAMES_DIR,
    mongo_secret_block: str = "mongo-connection-string",
    database_name: str = "cluebase",
):
    logger = get_run_logger()
    logger.info(f"Loading clues from game {game_id}")

    game_file_path = os.path.join(games_dir, f"{game_id}.html")

    bucket = get_s3_bucket(s3_bucket_name)

    clues = await read_and_parse_clues(bucket, game_file_path, logger)
    logger.info(f"Parsed {len(clues)} clues from {game_file_path}")

    logger.info(f"Getting Mongo connection using secret block {mongo_secret_block}")
    mongo_conn_str = (await Secret.load(mongo_secret_block)).get()
    mongo_client = await get_mongo_client(mongo_conn_str)
    db = mongo_client.get_database(database_name)

    logger.info(f"Attempting to load clues into collection")
    try:
        loaded = await insert_clue_bulk(db, clues)
        logger.info(f"Loaded {len(loaded)} clues from {game_file_path}")
        logger.info(loaded)
        return loaded
    except pymongo.errors.BulkWriteError as e:
        logger.info(f"Loaded {e.details['nInserted']} clues from {game_file_path}")
        logger.warning(e)
        return e.details["writeErrors"]


@flow
def load_clues_from_single_game_s3(
    game_id: str,
    s3_bucket_name="cluebase",
    s3_games_path="raw/games",
):
    asyncio.run(load_game_file_s3(game_id, s3_bucket_name, s3_games_path))
