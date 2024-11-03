from prefect import flow, task
from prefect.logging import get_run_logger

from src.io_utils import get_s3_bucket, read_s3_object
from src.paths import RAW_LIST_SEASONS
from src.scrape_raw import download_season_page_to_s3, parse_season_ids
from workflows.scrape.shared import refresh_games, refresh_season_list


@flow
def refresh_latest_season(bucket_name="cluebase", overwrite: bool = True):
    if bucket_name:
        print("Refreshing files in bucket: {cluebase}")

    prefect_logger = get_run_logger()

    refresh_season_list(bucket_name, overwrite=True, logger=prefect_logger)

    latest_season = download_latest_season(bucket_name)

    refresh_games(
        latest_season, bucket_name, overwrite=overwrite, logger=prefect_logger
    )


@task
def download_latest_season(bucket_name="cluebase", list_seasons_path=RAW_LIST_SEASONS):
    prefect_logger = get_run_logger()

    bucket = get_s3_bucket(bucket_name)
    list_seasons_html = read_s3_object(bucket, list_seasons_path)
    recent_season_id = parse_season_ids(list_seasons_html)[0]

    prefect_logger.info(f"Downloading latest season: {recent_season_id}")

    download_season_page_to_s3(recent_season_id, bucket, overwrite=True)

    return recent_season_id
