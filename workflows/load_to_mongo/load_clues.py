import asyncio
import os

import pymongo
from tqdm.asyncio import tqdm

from src.clues import parse_clues_from_game
from src.paths import RAW_GAMES_DIR
from src.utils import insert_clue_bulk


async def load_clues_from_game_file(game_file):
    game_id = game_file.split(".")[0]
    print(f"Loading clues from game: {game_id}")

    clues = parse_clues_from_game(game_id)

    await insert_clue_bulk(clues)


async def load_all_game_files(games_dir=RAW_GAMES_DIR):
    with tqdm(os.listdir(games_dir)) as progress:
        async for game_file in progress:
            try:
                await load_clues_from_game_file(game_file)
            except pymongo.errors.BulkWriteError as e:
                print(e)


if __name__ == "__main__":

    # asyncio.run(load_clues_from_game_file("6737.html"))

    asyncio.run(load_all_game_files())
