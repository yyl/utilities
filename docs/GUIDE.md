# Utilities Developer Guide

This document contains technical details, under-the-hood implementations, and architectural notes for the scripts in this repository.

## `statement_parser.py`

### Format Auto-Detection

The parser auto-detects the CSV source by inspecting the header row against known column sets:

| Source       | Required columns                                                                                |
|--------------|-------------------------------------------------------------------------------------------------|
| Wealthfront  | `Transaction date`, `Description`, `Type`, `Amount`                                             |
| Discover     | `Transaction Date`, `Transaction Description`, `Transaction Type`, `Debit`, `Credit`, `Balance` |

Detection is case-insensitive and uses subset matching, so extra columns are tolerated. If no known format matches, the parser raises an error.

### CSV Parsing

Both parsers normalise dates from `MM/DD/YYYY` to `YYYY-MM-DD` and unify amounts into a single signed float (negative = money leaving the account).

- **Wealthfront**: reads the `Amount` column directly (already signed).
- **Discover**: reads separate `Debit` and `Credit` columns (dollar-formatted, e.g. `$1,600.00`). The unified amount is `credit − debit`.

Each transaction is tagged with an `account_id` (`"wealthfront"` or `"discover"`) derived from the detected format.

### Inter-Account Transfer Cancellation

When importing from multiple sources in a single invocation, the parser detects inter-account transfers and removes them to avoid double-counting. The algorithm:

1. Groups all parsed transactions by `(date, abs(amount))`.
2. Within each group, greedily pairs one positive-amount transaction with one negative-amount transaction from a *different* `account_id`.
3. Both sides of a matched pair are removed from the import set.

This catches transfers like a Wealthfront deposit of `+2300` on 2025-01-17 paired with a Discover withdrawal of `−2300` on the same date.

### SQLite Schema & Deduplication

```sql
CREATE TABLE IF NOT EXISTS transactions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    date         TEXT NOT NULL,
    description  TEXT NOT NULL,
    amount       REAL NOT NULL,
    subtype      TEXT,
    category     TEXT NOT NULL DEFAULT 'transaction',
    account_id   TEXT,
    source_file  TEXT,
    imported_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(date, description, amount, account_id)
);
```

Unlike the QFX parser which uses `FITID` as a natural key, statement exports have no unique transaction identifier. Deduplication uses a composite unique constraint on `(date, description, amount, account_id)` with `INSERT OR IGNORE`. The `account_id` field now distinguishes transactions from different sources, so the same amount on the same date from different banks won't collide. Two genuinely identical transactions on the same day from the *same* account (same payee, same amount) would still be deduplicated — an acceptable trade-off given how rare that is in practice.

### Analysis Table & Description Normalization

After importing transactions, the script automatically rebuilds an `analysis` table from scratch. This single table uses a `group_type` discriminator column (`'total'`, `'description'`, or `'source_file'`) to store different annual aggregations while avoiding table sprawl.

To improve the quality of merchant-level groupings, the parser normalises descriptions using regex by stripping out variable artifacts such as trailing transaction IDs (`-00000000`), masked account numbers (`(Account ****0000)`, `XXXXXXXXX`), and replacing arbitrary hyphens/underscores with spaces. The raw string is preserved in the `description` column of `transactions`, while the cleaned string is stored in `normalized_description` and used for the `analysis` grouping.

## github_repo_stat.py

### Smart Cloning Architecture

Analyzing very large repositories (e.g., `openai/codex`) can result in excessively long clone times or network timeouts if cloned synchronously. To alleviate this, the script implements a dynamic dual-strategy approach. It initially queries the repository size via the GitHub REST API and picks the optimal path:

1. **Small Repositories (< 75 MB)**:
   - Performs a standard **full `git` clone**.
   - All statistics, including LOC changed per commit, are computed entirely locally using `git log --shortstat`.
   - If the full clone process somehow exceeds a 2-minute (`120s`) timeout, the script wipes the partial clone and falls back to the shallow strategy.

2. **Large Repositories (≥ 75 MB)**:
   - Performs a **shallow clone** (`--depth 1`) solely for computing the active file matrix and total lines of code.
   - Fetches full commit history **without blobs** (`--filter=blob:none`) to rapidly retrieve commit counts and project lifespan without downloading file histories.
   - Bypasses local `git log --shortstat` (which forces extremely slow lazy-fetching of blobs on blobless clones) in favor of fetching per-commit diffs via the **GitHub GraphQL API**. SHAs are batched by 50 and executed across up to 10 parallel threads.

### Environment & Token Loading

The script relies on `python-dotenv` natively to securely manage GitHub personal access tokens.

- It looks for `.env` specifically in the script's directory (`os.path.dirname(__file__)`), ensuring it can be executed from any `CWD` via `uv run`.
- It overrides ambient shells (`override=True`) to prevent stale shell tokens from silently shadowing the expected token in `.env`.
- A valid `GITHUB_TOKEN` is mandatory for the large-repository (GraphQL) fallback logic because GitHub's GraphQL API disallows unauthenticated requests.

### Robust Disk Cleanup

Because repositories can easily be hundreds of megabytes in size, the application ensures that temporary clone directories (`/tmp/ghstat_*`) never exhaust local disk space:

- Normal execution, keyboard interrupts (Ctrl+C), and runtime errors trigger standard cleanup via python `finally` blocks.
- An `atexit` registered handler acts as a backup for unhandled process exits.
- On startup, the script conducts a proactive sweep of the system's temporary directory, scanning for and purging orphaned directories leftover from any past abnormal terminations (like `kill -9` or hard OS crashes).

## `tax_return_parser.py`

### Extensible Dynamic DB Schema

The repository used an earlier schema architecture involving pre-mapped YAML structures to extract tax data manually. To allow users to effortlessly add lines and forms out of scope without ever updating Python logic, it incorporates a dynamic schema build model:

- The script looks up all defined row entries within the given `csv` string table.
- These fields are dynamically translated into purely lowercase syntax attributes `f_{form}_{line}` inside the database schema to represent exactly what the user inputs (e.g., `f_1040_1z`).
- Missing SQLite columns are subsequently built with active `ALTER TABLE tax_returns ADD COLUMN ...` statements safely before CSV reading proceeds to `UPSERT` injection.

### Missing Data Extensibility

Older forms will gracefully inherit updated tracking constraints without cascading query failures because the SQLite evolution is 100% backward compatible (the fields simply yield `NULL` when reading standard past-year models). The parser itself correctly validates all incoming string structures by recursively evaluating floating amounts for currency elements like `$` symbols, `(123)` negatives, and trailing whitespace sequences prior to injecting `REAL` variables locally.

Values of `NA`, `N/A`, `null`, or `-` in the CSV are treated as `NULL` in the database. This handles cases where a form line is not applicable to a particular tax year (e.g., a form that didn't exist that year) or where data is simply missing.

Rows with a `Form` value of `0` are treated as metadata (e.g., `0,0,Means,HR Block,...`) and skipped during schema and data extraction.

### Persisted Analysis Table

The parser now stores derived multi-year analysis in a second SQLite table, `tax_return_analysis`, inside the same database as `tax_returns`.

- `tax_returns` remains the source-of-truth table for imported values keyed by `tax_year`.
- `tax_return_analysis` is keyed by the same `tax_year` and stores a `computed_at` timestamp plus derived metric columns:
  - `effective_tax_rate_pct`
  - `capital_gain_short_vs_long_ratio_pct`
  - `ca_effective_tax_rate_pct`
- The table also receives one dynamic YoY column per imported field using the pattern `yoy_<db_col>_pct`, for example `yoy_f_1040_24_pct`.
- Like the main import table, analysis schema evolution is automatic via `ALTER TABLE`, so adding new CSV rows automatically creates matching YoY analysis columns the next time analysis is stored.

### Analysis Flow

The `analyze` command computes derived metrics from the imported rows and then persists them before printing the CLI report.

1. Read all rows from `tax_returns` ordered by `tax_year`.
2. Compute YoY percent change for each imported field using the prior year as the baseline.
3. Compute derived ratios:
   - Effective tax rate = `f_1040_24 / f_1040_15`
   - Capital gain short-vs-long ratio = `f_d_7 / f_d_15`
   - CA effective tax rate = `f_540_64 / f_540_19`
4. Upsert one row per year into `tax_return_analysis`.

### Notes On Interpretation

- Effective tax rate is intentionally defined here as `Total tax / Taxable income`, not `Total tax / Total income`.
- The first imported year has no prior-year baseline, so all `yoy_*` columns for that year remain `NULL`.
- Any division with a missing or zero denominator also yields `NULL`, which keeps the stored analysis explicit and query-safe.

## mp4_to_gif.py

### Backend Auto-Detection

`find_ffmpeg()` resolves an ffmpeg executable: first on `PATH` (`shutil.which`), then via the `imageio-ffmpeg` package (pulled in through the `imageio[ffmpeg]` extra in `pyproject.toml`). Its `get_ffmpeg_exe()` may attempt a network download on first use, so the probe is wrapped in a broad `try/except` that treats any failure as "not available". `--backend` forces `ffmpeg` or `imageio`; the default `auto` picks ffmpeg when a binary resolves, otherwise imageio. Resolution happens once per invocation — in batch mode the resolved backend is reused for every file rather than re-detecting per file, and the banner always prints the resolved backend.

### Two-Pass FFmpeg Backend

`convert_with_ffmpeg` runs ffmpeg's standard high-quality GIF pipeline inside a temp directory:

1. `palettegen=stats_mode=diff:max_colors=N` applied after the `fps=<fps>,scale=<width>:-2:flags=lanczos` filter chain writes a single palette PNG. `stats_mode=diff` biases the palette toward pixels that change between frames; lanczos preserves edges during downscale.
2. `paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle` re-encodes the stream against that palette. `diff_mode=rectangle` restricts re-quantization to the bounding rectangle of what actually changed, cutting inter-frame redundancy.

`--start`/`--end` are applied as *input-level* `-ss`/`-t` in both passes, so the generated palette matches exactly the frames that will be encoded.

### Imageio Fallback

`convert_with_imageio` needs no ffmpeg binary (imageio + Pillow only):

- Frames are resampled by walking the decoded stream and keeping every `src_fps / target_fps`-th frame, so output FPS matches `--fps` regardless of the source rate.
- One global palette is built by iteratively re-quantizing up to ~12 evenly spaced representative frames *without* dithering (dither-free passes keep the palette stable); all frames are then quantized against it with Floyd–Steinberg dithering.
- Frames save with `disposal=2`, which a shared global palette requires to avoid ghosting trails, plus `optimize=True`.
- A stream that would yield more than 2000 frames triggers a warning suggesting a shorter trim or lower fps.

### Size-Limit Retry Loop

`convert_mp4_to_gif` is the size-limit driver shared by both backends: convert, measure, and if the result exceeds `--max-size` call `compute_next_parameters` and re-convert (up to 5 attempts). The heuristic:

1. Width scales by `sqrt(target/current) × 0.9` (file size scales roughly with area; 10% safety margin), rounded to 10s and floored at 180px.
2. If that would drop width below 320px, framerate is also reduced (×0.8, min 5) to trade motion for resolution, and width is recomputed with the extra budget.
3. If width lands below 260px, the palette shrinks 256 → 128 colors.
4. A monotonicity guard forces at least one parameter to strictly decrease each round (80% width, −2 fps, or halved colors) so the loop can never stall in place; if the limit still isn't met after 5 attempts, a warning is printed and the last output is kept.

`--max-size 0` disables the loop entirely (single-shot conversion).

### Batch Mode

- `find_videos` recursively scans for the default suffix set (`.mp4 .mov .m4v .mkv .webm .avi`) or whatever `-p` supplies, sorted by filename.
- Each file converts inside a `try/except`; failures are reported and counted but never abort the batch.
- `make_output` resolves the `-o` template per file via `str.replace` rather than Python `%` formatting — a template containing `%i%` would be parsed as the int format specifier and crash under `%`-formatting. Placeholders: `%name%` (path stem — for `my.video.mp4` that is `my.video`), `%extname%` (lowercased extension), `%counter%`/`%i%` (1-based ordinal), `%index%` (1-based, zero-padded to 4 digits). A template without placeholders is used verbatim, and any resulting path lacking a `.gif` suffix gets one appended.
