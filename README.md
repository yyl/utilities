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
- `github_repo_stat.py`: Clones a GitHub repository to a temporary directory and analyzes its code and git history. It reports statistics including:
  - Total lines of code and file count.
  - Total commits and repository lifespan (in days).
  - Lines of Code (LOC) written per day.
  - Min/median/average/max statistics for lines per file and lines changed per commit.
  - Supports private repositories via a personal access token.
  - Run it with: `uv run github_repo_stat.py <repo_url> [--token <your_token>]`