from workflows.scrape.shared import refresh_games

OVERWRITE = False

refresh_games(overwrite=OVERWRITE, sleep=3)
