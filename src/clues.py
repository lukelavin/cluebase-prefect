import os
import re
from datetime import datetime

from bs4 import BeautifulSoup

from src.io_utils import read_raw_file
from src.paths import RAW_GAMES_DIR


def clue_quality_pass(clues):
    clue_text_not_empty = lambda clue: len(clue["clueText"]) > 0
    clue_text_not_equals = lambda clue: clue["clueText"] != "="
    clue_text_no_preamble = lambda clue: not re.search(r"^\(.*\)", clue["clueText"])
    clue_text_no_brackets = lambda clue: not re.search(r"\[.*\]", clue["clueText"])

    filter_rules = [
        clue_text_not_empty,
        clue_text_not_equals,
        clue_text_no_preamble,
        clue_text_no_brackets,
    ]

    for rule in filter_rules:
        clues = filter(rule, clues)

    clues = list(clues)

    for clue in clues:
        clue["category"] = reformat_category_commentary(clue["category"])

    return clues


def remove_category_commentary(category):
    if re.search(r"\n", category):
        print(category)
        return category.split("\n")[-1]
    else:
        return category


def reformat_category_commentary(category):
    reformatted_category = category

    match = re.search(r"\n[^a-z]+$", category)
    if match:
        return category.split("\n")[-1]

    match = re.search(r"(\()(\b.*\b.*:\s*).*(\))", category)
    if match:
        match.span(1)
        reformatted_category = (
            category[: match.span(1)[0]]  # before (
            + "["
            + category[match.span(1)[1] : match.span(2)[0]]  # from ( to host name
            + category[match.span(2)[1] : match.span(3)[0]]  # from end host name to )
            + "]"
            + category[match.span(3)[1] :]  # after )
        )

    match = re.search(r"(\n)(\b.*\b.*:\s*).*(\))", category)
    if match:
        match.span(1)
        reformatted_category = (
            category[: match.span(1)[0]]  # before \n
            + "\n["
            + category[match.span(1)[1] : match.span(2)[0]]  # from \n to host name
            + category[match.span(2)[1] : match.span(3)[0]]  # from end host name to )
            + "]"
            + category[match.span(3)[1] :]  # after )
        )

    match = re.search(r"(\()(\b.*\b.*:\s*).*", category)
    if match:
        match.span(1)
        reformatted_category = (
            category[: match.span(1)[0]]  # before \n
            + "["
            + category[match.span(1)[1] : match.span(2)[0]]  # from \n to host name
            + category[match.span(2)[1] :]  # from end host name to )
            + "]"
        )

    match = re.search(r"(\()(\b.*\b.*;\s*).*(\))", category)
    if match:
        match.span(1)
        reformatted_category = (
            category[: match.span(1)[0]]  # before (
            + "["
            + category[match.span(1)[1] : match.span(2)[0]]  # from ( to host name
            + category[match.span(2)[1] : match.span(3)[0]]  # from end host name to )
            + "]"
            + category[match.span(3)[1] :]  # after )
        )

    return reformatted_category


def build_clue_id(game_id, round, category_number, difficulty):
    return f"{str(game_id).zfill(6)}-{round}-{category_number}-{difficulty}"


def build_clue_dict(
    clue_text,
    solution,
    category,
    difficulty,
    air_date,
    game_id,
    round_number,
    category_index,
):
    return {
        "_id": build_clue_id(game_id, round_number, category_index, difficulty),
        "clueText": clue_text,
        "solution": solution,
        "category": category,
        "difficulty": difficulty,
        "airDate": air_date,
        "gameId": game_id,
        "roundNumber": round_number,
        "categoryIndex": category_index,
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
    }


def build_clues_from_row(row, row_num, categories, game_id, round_number):
    clue_tables = [td.find("table") for td in row.find_all("td", class_="clue")]

    clues = []
    for i, clue_table in enumerate(clue_tables):
        if not clue_table:
            continue

        clue_text = clue_table.find("td", class_="clue_text").text.strip()
        solution = clue_table.find(class_="correct_response").text.strip()
        category = categories[i]

        clue_dict = build_clue_dict(
            clue_text=clue_text,
            solution=solution,
            category=category,
            difficulty=row_num,
            air_date=None,
            game_id=game_id,
            round_number=round_number,
            category_index=i,
        )

        clues.append(clue_dict)

    return clues


def parse_round(round_table, round_number, game_id):
    categories = [
        cat.text.strip() for cat in round_table.find_all("td", class_="category")
    ]

    clue_rows = round_table.find_all("tr", recursive=False)[1:]

    clues = [
        clue
        for clue_list in [
            build_clues_from_row(row, row_num, categories, game_id, round_number)
            for row_num, row in enumerate(clue_rows)  # first row is category header
        ]
        for clue in clue_list
    ]

    qad_clues = clue_quality_pass(clues)

    return qad_clues


def parse_first_round(soup, game_id):
    first_round_table = soup.find("table", class_="round")
    return parse_round(first_round_table, 1, game_id)


def parse_second_round(soup, game_id):
    round_tables = soup.find_all("table", class_="round")
    if len(round_tables) > 1:
        second_round_table = soup.find_all("table", class_="round")[1]
        return parse_round(second_round_table, 2, game_id)
    else:
        return []


def parse_final_jeopardy(soup, game_id):
    round_number = 3
    category_index = 0
    difficulty = 5

    fj_table = soup.find("table", class_="final_round")
    if not fj_table:
        return []

    category = fj_table.find(class_="category_name").text.strip()
    clue_text = fj_table.find(id="clue_FJ").text.strip()
    solution = fj_table.find("em", class_="correct_response").text.strip()

    clue = build_clue_dict(
        clue_text=clue_text,
        solution=solution,
        category=category,
        difficulty=difficulty,
        air_date=None,
        game_id=game_id,
        round_number=round_number,
        category_index=category_index,
    )

    qad_clues = clue_quality_pass([clue])

    return qad_clues


def parse_air_date(soup):
    AIR_DATE_REGEX = r"[0-9]{4}-[0-9]{2}-[0-9]{2}"

    air_date_match = re.search(AIR_DATE_REGEX, soup.title.text)
    air_date_text = soup.title.text[air_date_match.start() : air_date_match.end()]
    air_date = datetime.strptime(air_date_text, "%Y-%m-%d")
    return air_date


def parse_clues_from_game(game_id):
    raw_html = read_raw_file(os.path.join(RAW_GAMES_DIR, f"{game_id}.html"))
    soup = BeautifulSoup(raw_html, "html.parser")

    air_date = parse_air_date(soup)

    first_round_clues = parse_first_round(soup, game_id)
    second_round_clues = parse_second_round(soup, game_id)
    final_jeopardy_clues = parse_final_jeopardy(soup, game_id)

    all_clues = first_round_clues + second_round_clues + final_jeopardy_clues

    for clue in all_clues:
        clue["airDate"] = air_date

    return all_clues
