from prefect import flow
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials

SOURCE_REPO = "https://github.com/lukelavin/jeop-db-2"


if __name__ == "__main__":

    github_repo = GitRepository(
        # configured token in github and then created github credentials block in prefect UI
        url=SOURCE_REPO,
        credentials=GitHubCredentials.load("github-cluebase2"),
    )

    # TODO: make these packages specific per flow to reduce unnecessary installs
    pip_packages = []
    with open("requirements.txt", "r") as f:
        pip_packages = f.read().splitlines()

    flow.from_source(
        source=github_repo, entrypoint="workflows/scrape/refresh_all.py:refresh_all"
    ).deploy(
        name="refresh-all-github",
        work_pool_name="my-work-pool",
        job_variables={"pip_packages": pip_packages},
    )

    flow.from_source(
        source=github_repo,
        entrypoint="workflows/scrape/refresh_latest_season.py:refresh_latest_season",
    ).deploy(
        name="refresh-latest-season-github",
        work_pool_name="my-work-pool",
        cron="0 2 * * *",
        job_variables={"pip_packages": pip_packages},
    )

    flow.from_source(
        source=github_repo,
        entrypoint="workflows/load_to_mongo/load_clues.py:load_clues_from_all_games_s3",
    ).deploy(
        name="load-all-clues-github",
        work_pool_name="my-work-pool",
        cron="0 5 * * *",
        job_variables={"pip_packages": pip_packages},
    )
