from src.utils import insert_clue_bulk
from src.clues import parse_clues_from_game

async def load_clues_from_game_file(game_file):
    game_id = game_file.split(".")[0]
    print(f"Loading clues from game: {game_id}")

    clues = parse_clues_from_game(game_id)

    await insert_clue_bulk(clues)


if __name__ == "__main__":
    # asyncio.run(load_clues_from_game_file("6737.html"))
