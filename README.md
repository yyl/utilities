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
- `github_repo_stat.py`: Fetch and print statistical information about GitHub repositories.
  - Run it with: `uv run github_repo_stat.py [options]`