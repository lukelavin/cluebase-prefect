from prefect import flow
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials

SOURCE_REPO = "https://github.com/lukelavin/jeop-db-2"

# TODO: Deployment w/ git (maybe fix project structure/dependencies)
if __name__ == "__main__":

    github_repo = GitRepository(
        # configured token in github and then created github credentials block in prefect UI
        url=SOURCE_REPO,
        credentials=GitHubCredentials.load("github-cluebase2"),
    )

    pip_packages = []
    with open("requirements.txt", "r") as f:
        pip_packages = f.read().splitlines()

    flow.from_source(
        source=github_repo, entrypoint="workflows/scrape/refresh_all.py:refresh_all"
    ).deploy(
        name="github-deploy",
        work_pool_name="my-work-pool",
        cron="0 1 * * *",
        job_variables={"pip_packages": pip_packages},
    )
