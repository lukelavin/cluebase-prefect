from prefect import flow

from workflows.load_to_mongo.load_clues_set import load_clues_from_set_s3
from workflows.ml_features.classify_domain import classify_domains
from workflows.scrape.refresh_latest_season import refresh_latest_season


@flow
def parse_and_load_latest_season(
    bucket_name="cluebase", s3_games_path="raw/games", overwrite: bool = True
):
    game_ids = refresh_latest_season(bucket_name, overwrite)

    load_clues_from_set_s3(game_ids, bucket_name, s3_games_path)

    classify_domains()
