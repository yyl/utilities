# Utilities Developer Guide

This document contains technical details, under-the-hood implementations, and architectural notes for the scripts in this repository.

## `statement_parser.py`

### Format Auto-Detection

The parser auto-detects the CSV source by inspecting the header row against known column sets:

| Source       | Required columns                                                                   |
|--------------|------------------------------------------------------------------------------------|
| Wealthfront  | `Transaction date`, `Description`, `Type`, `Amount`                                |
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
