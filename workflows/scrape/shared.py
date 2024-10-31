import time
import random
from tqdm import tqdm
from src.scrape_raw import (
    parse_all_game_ids,
    parse_season_urls,
    download_game_page,
    download_season_page,
)


def refresh_seasons(overwrite=False, sleep="random"):
    season_ids = parse_season_urls()

    for id in tqdm(season_ids):
        success = download_season_page(id, overwrite=overwrite)
        if success:
            if sleep == "random":
                time.sleep(random.uniform(0.2, 2.0))
            else:
                time.sleep(sleep)


def refresh_games(overwrite=False, sleep="random"):
    game_ids = parse_all_game_ids()

    for game_id in tqdm(game_ids):
        success = download_game_page(game_id, overwrite=overwrite)
        if success:
            if sleep == "random":
                time.sleep(random.uniform(0.2, 2.0))
            else:
                time.sleep(sleep)
