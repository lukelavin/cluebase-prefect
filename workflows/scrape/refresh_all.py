from prefect import flow
from prefect.logging import get_run_logger

from workflows.scrape.shared import (
    refresh_all_games,
    refresh_all_seasons,
    refresh_season_list,
)


@flow
def refresh_all(bucket_name="cluebase", overwrite: bool = True):
    if bucket_name:
        print("Refreshing files in bucket: {cluebase}")

    prefect_logger = get_run_logger()

    refresh_season_list(bucket_name, overwrite=True, logger=prefect_logger)

    refresh_all_seasons(bucket_name, overwrite=True, logger=prefect_logger)

    refresh_all_games(bucket_name, overwrite=overwrite, logger=prefect_logger)
