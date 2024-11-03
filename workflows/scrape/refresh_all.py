from prefect import flow, task
from prefect.logging import get_run_logger

from src.scrape_raw import download_season_list
from workflows.scrape.shared import refresh_games, refresh_season_list, refresh_seasons


@flow(log_prints=True)
def refresh_all(bucket_name="cluebase", overwrite: bool = True):
    if bucket_name:
        print("Refreshing files in bucket: {cluebase}")

    prefect_logger = get_run_logger()

    refresh_season_list(bucket_name, overwrite=False, logger=prefect_logger)

    refresh_seasons(bucket_name, overwrite=False, logger=prefect_logger)

    refresh_games(bucket_name, overwrite=overwrite, logger=prefect_logger)


# if __name__ == "__main__":
#     refresh_all()
