import random
import time

from prefect import task
from tqdm import tqdm

from src.paths import RAW_LIST_SEASONS, RAW_SEASONS_DIR
from src.scrape_raw import (
    download_game_page,
    download_season_list,
    download_season_page,
    parse_all_game_ids,
    parse_season_ids,
)

## TODO: surface parameters from functions all the way up to workflows
## TODO: turn prints into logging with proper log levels

@task
def refresh_season_list(overwrite=False):
    return download_season_list(overwrite=True)

@task
def refresh_seasons(overwrite=True, sleep="random", list_seasons_file=RAW_LIST_SEASONS):
    season_ids = parse_season_ids(list_seasons_file)

    for id in tqdm(season_ids):
        success = download_season_page(id, overwrite=overwrite)
        if success:
            if sleep == "random":
                time.sleep(random.uniform(0.2, 2.0))
            else:
                time.sleep(sleep)

@task
def refresh_games(overwrite=False, sleep="random", raw_seasons_dir=RAW_SEASONS_DIR):
    game_ids = parse_all_game_ids(raw_seasons_dir)

    for game_id in tqdm(game_ids):
        success = download_game_page(game_id, overwrite=overwrite)
        if success:
            if sleep == "random":
                time.sleep(random.uniform(0.2, 2.0))
            else:
                time.sleep(sleep)
