from prefect import flow, task

from src.scrape_raw import download_season_list
from workflows.scrape.shared import refresh_games, refresh_season_list, refresh_seasons


@flow(log_prints=True)
def refresh_all(bucket_name="cluebase", overwrite: bool = False):
    if bucket_name:
        print("Refreshing files in bucket: {cluebase}")

    refresh_season_list(bucket_name, overwrite=False)

    refresh_seasons(bucket_name, overwrite=False)

    refresh_games(bucket_name, overwrite=overwrite)


# if __name__ == "__main__":
#     refresh_all()
