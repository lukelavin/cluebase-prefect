import random
import time
from logging import getLogger

from prefect import task
from tqdm import tqdm

from src.io_utils import get_s3_bucket, read_s3_object
from src.paths import RAW_LIST_SEASONS, RAW_SEASONS_DIR
from src.scrape_raw import (
    download_game_page,
    download_game_page_to_s3,
    download_season_list,
    download_season_list_to_s3,
    download_season_page,
    download_season_page_to_s3,
    parse_all_game_ids,
    parse_all_game_ids_from_s3,
    parse_game_ids,
    parse_season_ids,
)

## TODO: surface parameters from functions all the way up to workflows
## TODO: turn prints into logging with proper log levels

file_logger = getLogger(__name__)


@task
def refresh_season_list(s3_bucket_name="cluebase", overwrite=True, logger=file_logger):
    if s3_bucket_name:
        bucket = get_s3_bucket(s3_bucket_name)

        return download_season_list_to_s3(bucket, overwrite=overwrite)

    return download_season_list(overwrite=overwrite)


@task
def refresh_all_seasons(
    s3_bucket_name="cluebase",
    overwrite=True,
    sleep="random",
    list_seasons_file=RAW_LIST_SEASONS,
    logger=file_logger,
):
    if s3_bucket_name:
        bucket = get_s3_bucket(s3_bucket_name)
        list_seasons_html = read_s3_object(bucket, list_seasons_file)
        season_ids = parse_season_ids(list_seasons_html)

        for id in tqdm(season_ids):
            success = download_season_page_to_s3(id, bucket, overwrite=overwrite)

            if success:
                if sleep == "random":
                    time.sleep(random.uniform(0.2, 2.0))
                else:
                    time.sleep(sleep)
    else:
        list_seasons_html = None
        with open(list_seasons_file, "r") as f:
            list_seasons_html = f.read()

        season_ids = parse_season_ids(list_seasons_html)

        for id in tqdm(season_ids):
            success = download_season_page(id, overwrite=overwrite)

            if success:
                if sleep == "random":
                    time.sleep(random.uniform(0.2, 2.0))
                else:
                    time.sleep(sleep)


@task
def refresh_games(
    season_id, s3_bucket_name="cluebase", overwrite=False, logger=file_logger
):

    bucket = get_s3_bucket(bucket_name=s3_bucket_name)

    season_html = read_s3_object(bucket, f"{RAW_SEASONS_DIR}/{season_id}.html")

    game_ids = parse_game_ids(season_html)

    logger.info(f"Parsed {len(game_ids)} game ids for season {season_id}")
    logger.debug(game_ids)

    skipped = 0
    downloaded = 0
    for game_id in tqdm(game_ids):
        success = download_game_page_to_s3(
            game_id, bucket, overwrite=overwrite, logger=logger
        )
        if success:
            downloaded += 1
            time.sleep(random.uniform(0.2, 2.0))
        else:
            skipped += 1

    logger.info(f"Downloaded {downloaded} games, skipped {skipped} games")


@task
def refresh_all_games(
    s3_bucket_name="cluebase",
    overwrite=False,
    sleep="random",
    raw_seasons_dir=RAW_SEASONS_DIR,
    logger=file_logger,
):
    skipped = 0
    downloaded = 0

    if s3_bucket_name:
        bucket = get_s3_bucket(s3_bucket_name)

        game_ids = parse_all_game_ids_from_s3(bucket, raw_seasons_dir, logger=logger)

        for game_id in tqdm(game_ids):
            success = download_game_page_to_s3(
                game_id, bucket, overwrite=overwrite, logger=logger
            )
            if success:
                downloaded += 1
                if sleep == "random":
                    time.sleep(random.uniform(0.2, 2.0))
                else:
                    time.sleep(sleep)
            else:
                skipped += 1

    else:
        game_ids = parse_all_game_ids(raw_seasons_dir, logger=logger)

        for game_id in tqdm(game_ids):
            success = download_game_page(game_id, overwrite=overwrite, logger=logger)
            if success:
                downloaded += 1
                if sleep == "random":
                    time.sleep(random.uniform(0.2, 2.0))
                else:
                    time.sleep(sleep)
            else:
                skipped += 1

    logger.info(f"Downloaded {downloaded} games, skipped {skipped} games")
