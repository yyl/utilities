# utilities

A bunch of scripts that do stuff. This repository uses [`uv`](https://github.com/astral-sh/uv) for dependency management.

## Setup

First, install [uv](https://docs.astral.sh/uv/getting-started/installation/) if you haven't already. Then run:

```bash
uv sync
```

## Scripts

- `read_parquet.py`: This script reads a Parquet file, extracts and prints its schema using PyArrow, and then reads and prints the first few rows using Polars, providing a quick overview of the file's structure and content.
  - Run it with: `uv run read_parquet.py <file_path>`
- `github_repo_stat.py`: Analyzes a GitHub repository's code and git history. Reports:
  - Total lines of code, file count, commit count, and repository lifespan.
  - LOC written per day.
  - Min/median/average/max for lines per file and lines changed per commit.
  - **Smart cloning**: checks repo size via the GitHub API and picks the fastest strategy:
    - **< 75 MB**: full clone (all stats computed locally).
    - **≥ 75 MB**: shallow clone for file stats, GitHub GraphQL API for per-commit diff stats.
    - If a full clone exceeds a 2-minute timeout, it automatically falls back to the shallow strategy.
  - **Token**: reads `GITHUB_TOKEN` from a `.env` file in the script directory, the environment, or `--token`. A token is required for per-commit diff stats on large repos (GraphQL API) and for private repos.
  - **Cleanup**: cloned repos are removed on exit, interrupt, or error. Stale temp dirs from previous crashed runs are cleaned up automatically on the next run.
  - Run it with: `uv run github_repo_stat.py <repo_url> [--token <your_token>]`