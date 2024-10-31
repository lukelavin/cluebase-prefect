from shared import refresh_games, refresh_seasons

from src.scrape_raw import download_season_list

OVERWRITE = True

download_season_list(overwrite=OVERWRITE)

refresh_seasons(overwrite=OVERWRITE)

refresh_games(overwrite=OVERWRITE)
