# utilities

A bunch of scripts that do stuff.

## Setup

First, install [uv](https://docs.astral.sh/uv/getting-started/installation/) if you haven't already. Then run:

```bash
uv sync
```

## Scripts

### `read_parquet.py`

This script reads a Parquet file, extracts and prints its schema using PyArrow, and then reads and prints the first few rows using Polars, providing a quick overview of the file's structure and content.

- Run it with: `uv run read_parquet.py <file_path>`

---

### `statement_parser.py`

Parses bank statement CSVs from multiple sources and imports them into a unified SQLite database. Auto-detects the CSV format from the header row.

**Supported sources:**

- **Wealthfront** — columns: `Transaction date, Description, Type, Amount`
- **Discover** — columns: `Transaction Date, Transaction Description, Transaction Type, Debit, Credit, Balance`

When importing from multiple sources at once, inter-account transfers (same date, matching amount, opposite signs across different accounts) are automatically cancelled to prevent double-counting. Deduplicates on `(date, description, amount, account_id)` — re-importing the same file is safe.

After importing, automatically generates an `analysis` table with annual aggregations (totals, grouped by normalized description, and grouped by source file).

- Run it with: `uv run statement_parser.py data/statements/wf_202501.csv`
- Import multiple sources together: `uv run statement_parser.py data/statements/wf_202501.csv data/statements/discover_202501.csv`
- Glob all statements: `uv run statement_parser.py data/statements/*.csv`
- Optional flags:
  - `--db <path>` to specify the SQLite database path (default: `data/transactions.db`).

---

### `health_workout_extract.py`

Streams an Apple Health `export.xml` file in two passes and writes a much smaller XML containing all `Workout` elements plus overlapping `Record` and `Correlation` entries.

- Run it with: `uv run health_workout_extract.py export.xml [workouts_only.xml]`
- Optional flags:
  - `--include-activity-summaries` to keep `ActivitySummary` entries too.
  - `--types <RecordType> ...` to force-include specific top-level `Record` types even if they fall outside workout time windows.

---

### `github_repo_stat.py`

Analyzes a GitHub repository's code and git history. Reports:

- Total lines of code, file count, commit count, and repository lifespan.
- LOC written per day.
- Min/median/average/max for lines per file and lines changed per commit.

**Note**: A GitHub PAT token is required for private repositories, and highly recommended for large public repositories. Provide `GITHUB_TOKEN` via a `.env` file in the script directly, the environment, or the `--token` flag.
The script outputs a summary to the console and automatically writes out a `.txt` report into the `stats/` directory.

- Run it with: `uv run github_repo_stat.py <repo_url> [--token <your_token>]`

---

### `tax_return_parser.py`

Imports tax return data from a standardized local CSV file (exported from Google Sheets/Excel) into a local SQLite database for multi-year analysis.

- Dynamically builds the SQLite schema based on the custom rows you define in the CSV (`Form` + `Line`).
- Reads data from any column whose header is a 4-digit year.
- Values of "NA" or "N/A" in the CSV are treated as NULL (not applicable or missing).
- Stores imported return data in `tax_returns` and derived analysis data in `tax_return_analysis` within the same SQLite database.
- Computes and saves:
  - YoY percent change for every imported field
  - Effective tax rate as `Total tax / Taxable income`
  - Capital gain short-vs-long ratio as `Schedule D net short-term / net long-term`
  - CA effective tax rate as `CA total tax / CA taxable income`
- By default, looks for `data/tax_return/returns.csv`.
- Run the import with: `uv run tax_return_parser.py import` (or explicitly: `uv run tax_return_parser.py import --file data/tax_return/returns.csv`)
- Generate and persist the analysis with: `uv run tax_return_parser.py analyze`

(See [docs/GUIDE.md](docs/GUIDE.md) for technical setup and architectural details under the hood.)
