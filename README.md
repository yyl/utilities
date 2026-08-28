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

---

### `mp4_to_gif.py`

Converts video files (`.mp4`, `.mov`, `.m4v`, `.mkv`, `.webm`, `.avi`) into animated GIFs.

- Uses `ffmpeg` (from `PATH` or bundled with `imageio-ffmpeg`) for high-quality two-pass palette conversion when available; otherwise falls back to pure-Python `imageio` + Pillow automatically.
- Keeps the GIF under a size limit (default 10 MB) by automatically re-encoding at reduced width, framerate, and color count until it fits.
- Batch mode recursively processes every matching video, including files with the same name in different subdirectories. It continues after conversion failures and exits nonzero if any file fails.

- Run it with: `uv run mp4_to_gif.py video.mp4`
- Optional flags:
  - `-o <path or template>` output path (single mode) or filename template (batch mode; parent directories are created automatically)
  - `--fps <n>` output frame rate (default: 10)
  - `--width <px>` output width, aspect ratio preserved (default: 480)
  - `--max-size <mb>` output size limit in MB, `0` to disable (default: 10)
  - `--start <s>` / `--end <s>` trim range in seconds
  - `--loop <n>` GIF loop count, 0 = infinite
  - `--backend auto|ffmpeg|imageio` force a conversion backend
  - `-p .mp4 .mov` restrict extensions (batch mode only)
- Batch mode: `uv run mp4_to_gif.py --batch ./clips/ -o "recap_%index%_%name%.gif"`. Templates support the placeholders `%name%`, `%extname%`, `%counter%`, `%i%`, and `%index%`.

---

### `doc_to_markdown.py`

Downloads developer documentation, extracts its main content, and converts it to Markdown with a source URL, absolute links, headings, code blocks, lists, and tables preserved for agent consumption.

- Print one converted document: `uv run doc_to_markdown.py https://docs.foursquare.com/fsq-developers-places/reference/authentication`
- Write one document: `uv run doc_to_markdown.py <url> -o docs/authentication.md`
- Convert several URLs into a directory: `uv run doc_to_markdown.py <url1> <url2> -o docs/extracted/`
- Use a one-off CSS selector: `uv run doc_to_markdown.py <url> --selector "article.docs#content"`
- Configure site-wide selectors in `doc_content_selectors.json`, which maps URL prefixes to CSS selectors. The built-in Foursquare mapping uses `article.rm-Article#content`; the longest matching prefix wins.
