from prefect import flow, task

from src.scrape_raw import download_season_list
from workflows.scrape.shared import refresh_games, refresh_season_list, refresh_seasons


@flow(log_prints=True)
def refresh_all(overwrite: bool = False):
    refresh_season_list(overwrite=True)

    refresh_seasons(overwrite=True)

    refresh_games(overwrite=overwrite)


# if __name__ == "__main__":
#     refresh_all()
