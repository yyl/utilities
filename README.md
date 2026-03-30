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
  - **Note**: A GitHub PAT token is required for private repositories, and highly recommended for large public repositories. Provide `GITHUB_TOKEN` via a `.env` file in the script directly, the environment, or the `--token` flag.
  - The script outputs a summary to the console and automatically writes out a `.txt` report into the `stats/` directory.
  - Run it with: `uv run github_repo_stat.py <repo_url> [--token <your_token>]`

(See [docs/GUIDE.md](docs/GUIDE.md) for technical setup and architectural details under the hood.)