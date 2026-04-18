# Utilities Developer Guide

This document contains technical details, under-the-hood implementations, and architectural notes for the scripts in this repository.

## `statement_parser.py`

### PDF Text Extraction

The parser uses `pdfplumber` to extract text from each page of a Wealthfront bank statement PDF. Table extraction (`extract_tables`) was tested but only captures headers — the actual transaction rows render as free-form text, so regex-based line parsing is used instead.

### Transaction Line Format

Each transaction in the PDF spans two lines:

```
MM/DD/YYYY DESCRIPTION + $X,XXX.XX
Subtype
```

The regex `TRANSACTION_RE` captures date, description, sign (+/-), and amount from the first line. A lookahead checks the next line for a subtype keyword (Debit, Deposit, Credit, Transfer, Interest, Fee, Withdrawal).

### Section Detection

The parser tracks whether it's inside the "TRANSACTIONS" or "SWEEP TRANSACTIONS" section to populate the `category` field. Sweep transactions are internal Green Dot ↔ Wealthfront Cash Account settlements and are stored separately from user-facing transactions.

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

Unlike the QFX parser which uses `FITID` as a natural key, statement PDFs have no unique transaction identifier. Deduplication uses a composite unique constraint on `(date, description, amount, account_id)` with `INSERT OR IGNORE`. This means two genuinely identical transactions on the same day (same payee, same amount) would be deduplicated — an acceptable trade-off given how rare that is in practice.

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
