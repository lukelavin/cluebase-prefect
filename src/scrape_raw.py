import os
from typing import List

from bs4 import BeautifulSoup

from src.io_utils import download_html, download_html_to_s3, ls_s3
from src.paths import (
    RAW_DIR,
    RAW_GAMES_DIR,
    RAW_LIST_SEASONS,
    RAW_LIST_SEASONS_NAME,
    RAW_SEASONS_DIR,
)
from src.urls import BASE_URL, GAME, LIST_SEASONS, SEASON


def parse_season_urls(list_seasons_html: str) -> List[str]:
    soup = BeautifulSoup(list_seasons_html, "html.parser")
    season_table = soup.find("table")
    return [link.get("href") for link in season_table.find_all("a")]


def parse_season_ids(list_seasons_html: str) -> List[str]:
    soup = BeautifulSoup(list_seasons_html, "html.parser")
    season_table = soup.find("table")
    return [link.get("href").split("=")[-1] for link in season_table.find_all("a")]


def parse_game_ids(season_page_html: str) -> List[str]:
    soup = BeautifulSoup(season_page_html, "html.parser")
    game_table = soup.find("table")
    return list(
        filter(
            lambda game_id: game_id,
            [
                (
                    link.get("href").split("=")[-1]
                    if not link.get("href").startswith("http")
                    else None
                )
                for link in game_table.find_all("a")
            ],
        )
    )


def parse_all_game_ids(season_page_dir=RAW_SEASONS_DIR):
    game_ids = []
    for season_file in os.listdir(season_page_dir):
        print(f"Parsing game IDs from {os.path.join(season_page_dir, season_file)}")
        game_ids += parse_game_ids(os.path.join(season_page_dir, season_file))

    return game_ids


def parse_all_game_ids_from_s3(bucket, season_page_dir=RAW_SEASONS_DIR):
    game_ids = []
    for season_file in ls_s3(bucket, season_page_dir):
        print(f"Parsing game IDs from {season_file}")
        game_ids += parse_game_ids(season_file)

    return game_ids


def download_season_list(
    target_dir=RAW_DIR, target_filename=RAW_LIST_SEASONS_NAME, overwrite=False
):
    """
    Download Season List page.

    Includes urls to pages of all seasons. Effectively the root of the page tree.
    """
    print("Downloading Season List page")
    os.makedirs(target_dir, exist_ok=True)

    target_path = os.path.join(target_dir, target_filename)

    return download_html(BASE_URL + LIST_SEASONS, target_path, overwrite=overwrite)


def download_season_list_to_s3(bucket, target_path=RAW_LIST_SEASONS, overwrite=False):
    """
    Download Season List page to s3 bucket.

    Includes urls to pages of all seasons. Effectively the root of the page tree.
    """
    print("Downloading Season List page")

    return download_html_to_s3(
        BASE_URL + LIST_SEASONS, bucket, target_path, overwrite=overwrite
    )


def download_season_page(
    season_id, target_dir=RAW_SEASONS_DIR, target_filename=None, overwrite=False
):
    """
    Download Season page.

    Includes game urls as well as game dates and competitors
    """
    os.makedirs(target_dir, exist_ok=True)

    target_path = (
        os.path.join(target_dir, target_filename)
        if target_filename
        else os.path.join(target_dir, f"{season_id}.html")
    )

    return download_html(
        f"{BASE_URL}{SEASON}{season_id}", target_path, overwrite=overwrite
    )


def download_season_page_to_s3(
    season_id, bucket, target_dir=RAW_SEASONS_DIR, target_filename=None, overwrite=False
):
    """
    Download Season page to s3 bucket.

    Includes game urls as well as game dates and competitors
    """

    target_path = (
        os.path.join(target_dir, target_filename)
        if target_filename
        else os.path.join(target_dir, f"{season_id}.html")
    )

    return download_html_to_s3(
        f"{BASE_URL}{SEASON}{season_id}", bucket, target_path, overwrite=overwrite
    )


def download_game_page(
    game_id, target_dir=RAW_GAMES_DIR, target_filename=None, overwrite=False
):
    """
    Download Game page.

    Includes full clue tables
    """
    os.makedirs(target_dir, exist_ok=True)

    target_path = (
        os.path.join(target_dir, target_filename)
        if target_filename
        else os.path.join(target_dir, f"{game_id}.html")
    )

    return download_html(f"{BASE_URL}{GAME}{game_id}", target_path, overwrite=overwrite)


def download_game_page_to_s3(
    game_id, bucket, target_dir=RAW_GAMES_DIR, target_filename=None, overwrite=False
):
    """
    Download Game page to s3 bucket.

    Includes full clue tables
    """

    target_path = (
        os.path.join(target_dir, target_filename)
        if target_filename
        else os.path.join(target_dir, f"{game_id}.html")
    )

    return download_html_to_s3(
        f"{BASE_URL}{GAME}{game_id}", bucket, target_path, overwrite=overwrite
    )
