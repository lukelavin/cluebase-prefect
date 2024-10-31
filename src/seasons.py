from bs4 import BeautifulSoup


def parse_page_from_file(file_path):
    with open(file_path, "r") as html_file:
        html = html_file.read()
        soup = BeautifulSoup(html, "html.parser")
        # print(soup)

        rows = soup.find("table").findAll("tr")
        seasons_info = [tuple(td.text for td in row.findAll("td")) for row in rows]
        print(
            [
                season_info[0] + season_info[3] if len(season_info) > 3 else None
                for season_info in seasons_info
            ]
        )


if __name__ == "__main__":
    parse_page_from_file("downloaded.html")
