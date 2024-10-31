from prefect import flow
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials

SOURCE_REPO = "https://github.com/lukelavin/jeop-db-2"

# TODO: Deployment w/ git (maybe fix project structure/dependencies)
if __name__ == "__main__":

    github_repo = GitRepository(
        url=SOURCE_REPO, credentials=GitHubCredentials.load("github-cluebase2")
    )

    flow.from_source(
        source=github_repo, entrypoint="workflows/scrape/refresh_all.py:refresh_all"
    ).deploy(name="refresh_all_github_deploy", work_pool_name="my-work-pool")
    pass
