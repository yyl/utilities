#!/usr/bin/env python3
"""
Tax Return Parser — Import tax return data from a CSV spreadsheet into SQLite.

Usage:
    python tax_return_parser.py import
    python tax_return_parser.py analyze
"""

from __future__ import annotations

import os
import re
import sqlite3
import csv
from datetime import datetime, timezone

import click


# ── Database & Schema ────────────────────────────────────────────────────────

def sanitize_col(form: str, line: str) -> str:
    """Generate a safe SQLite column name from form and line (e.g., f_1040_1z)."""
    raw = f"f_{form}_{line}".lower().replace("-", "_")
    return re.sub(r'[^a-z0-9_]', '', raw)


def init_db(db_path: str, fields: list[tuple[str, str, str, str]]) -> None:
    """Create the tax_returns table and dynamically evolve the schema."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tax_returns (
                tax_year    INTEGER PRIMARY KEY,
                imported_at TEXT NOT NULL
            )
        """)
        
        cursor = conn.execute("PRAGMA table_info(tax_returns)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        
        for db_col, _, _, _ in fields:
            if db_col not in existing_cols:
                conn.execute(f"ALTER TABLE tax_returns ADD COLUMN {db_col} REAL")


def upsert_return(db_path: str, year: int, data: dict, db_cols: list[str]) -> None:
    """Insert or replace a tax return record in the database."""
    now = datetime.now(timezone.utc).isoformat()
    cols = ["tax_year", "imported_at"] + db_cols
    vals = [year, now] + [data.get(col) for col in db_cols]
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT OR REPLACE INTO tax_returns ({col_names}) VALUES ({placeholders})", vals
        )


def get_all_returns(db_path: str) -> list[dict]:
    """Fetch all tax return records."""
    if not os.path.exists(db_path):
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM tax_returns ORDER BY tax_year").fetchall()
        return [dict(row) for row in rows]


def init_analysis_db(db_path: str, fields: list[tuple[str, str, str, str]]) -> None:
    """Create the tax_return_analysis table and dynamically evolve the schema."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    derived_cols = [
        "effective_tax_rate_pct",
        "capital_gain_short_vs_long_ratio_pct",
        "ca_effective_tax_rate_pct",
    ]
    yoy_cols = [f"yoy_{db_col}_pct" for db_col, _, _, _ in fields]

    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tax_return_analysis (
                tax_year    INTEGER PRIMARY KEY,
                computed_at TEXT NOT NULL
            )
        """)

        cursor = conn.execute("PRAGMA table_info(tax_return_analysis)")
        existing_cols = {row[1] for row in cursor.fetchall()}

        for col in derived_cols + yoy_cols:
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE tax_return_analysis ADD COLUMN {col} REAL")


def upsert_analysis(db_path: str, analysis: dict, fields: list[tuple[str, str, str, str]]) -> None:
    """Insert or replace one analysis row in the database."""
    now = datetime.now(timezone.utc).isoformat()
    derived_cols = [
        "effective_tax_rate_pct",
        "capital_gain_short_vs_long_ratio_pct",
        "ca_effective_tax_rate_pct",
    ]
    yoy_cols = [f"yoy_{db_col}_pct" for db_col, _, _, _ in fields]
    yoy_values = {
        f"yoy_{db_col}_pct": analysis["yoy_changes_pct"].get(db_col)
        for db_col, _, _, _ in fields
    }

    cols = ["tax_year", "computed_at"] + derived_cols + yoy_cols
    vals = [analysis["tax_year"], now]
    vals.extend(analysis.get(col) for col in derived_cols)
    vals.extend(yoy_values.get(col) for col in yoy_cols)

    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"INSERT OR REPLACE INTO tax_return_analysis ({col_names}) VALUES ({placeholders})",
            vals,
        )


def pct_change(current: float | None, previous: float | None) -> float | None:
    """Calculate percent change from previous to current."""
    if current is None or previous is None or previous == 0:
        return None
    return ((current - previous) / abs(previous)) * 100


def ratio_pct(numerator: float | None, denominator: float | None) -> float | None:
    """Calculate a ratio as a percentage."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return (numerator / denominator) * 100


def format_metric(value: float | None) -> str:
    """Format a derived metric percentage for CLI output."""
    return f"{value:.1f}%" if value is not None else "N/A"


def build_analysis(records: list[dict], fields: list[tuple[str, str, str, str]]) -> tuple[list[dict], list[tuple[str, str]]]:
    """Create derived analysis rows and YoY changes for each imported field."""
    analyses = []
    previous_record = None

    for record in sorted(records, key=lambda r: r["tax_year"]):
        analysis = {
            "tax_year": record["tax_year"],
            "effective_tax_rate_pct": ratio_pct(record.get("f_1040_24"), record.get("f_1040_15")),
            "capital_gain_short_vs_long_ratio_pct": ratio_pct(record.get("f_d_7"), record.get("f_d_15")),
            "ca_effective_tax_rate_pct": ratio_pct(record.get("f_540_64"), record.get("f_540_19")),
            "yoy_changes_pct": {},
        }

        for db_col, desc, form, line in fields:
            prev_val = previous_record.get(db_col) if previous_record else None
            analysis["yoy_changes_pct"][db_col] = pct_change(record.get(db_col), prev_val)

        analyses.append(analysis)
        previous_record = record

    derived_metrics = [
        ("effective_tax_rate_pct", "Effective tax rate"),
        ("capital_gain_short_vs_long_ratio_pct", "Capital gain short vs long-term ratio"),
        ("ca_effective_tax_rate_pct", "CA effective tax rate"),
    ]
    return analyses, derived_metrics


def store_analysis(db_path: str, analyses: list[dict], fields: list[tuple[str, str, str, str]]) -> None:
    """Persist all analysis rows into the same SQLite database."""
    init_analysis_db(db_path, fields)
    for analysis in analyses:
        upsert_analysis(db_path, analysis, fields)


# ── CSV Parsing ─────────────────────────────────────────────────────────────

def parse_value(val: str) -> float | None:
    """Parse a value from CSV into a float, handling strings with $, commas, and NA."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = val.strip().replace("$", "").replace(",", "")
        if not cleaned or cleaned.lower() in ("null", "na", "n/a") or cleaned == "-":
            return None
        match = re.match(r"^\((.+)\)$", cleaned)
        if match:
            cleaned = "-" + match.group(1)
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def extract_schema_from_csv(filepath: str) -> tuple[list[tuple[str, str, str, str]], list[str], list[dict]]:
    """
    Returns (fields, year_cols, raw_rows).
    fields is a list of tuples: (db_col, description, form, line)
    """
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        
        # Verify mandatory columns
        if "Form" not in headers or "Line" not in headers:
            raise ValueError("CSV must contain 'Form' and 'Line' header columns.")

        desc_header = None
        if "Description" in headers:
            desc_header = "Description"
        elif "Year" in headers:
            desc_header = "Year"  # Legacy header name
        else:
            # Fallback to the 3rd column if possible
            desc_header = headers[2] if len(headers) > 2 else None
            
        year_cols = [h for h in headers if re.match(r"^\d{4}$", h.strip())]
        
        fields = []
        rows = list(reader)
        for row in rows:
            form = row.get("Form", "").strip()
            line = row.get("Line", "").strip()
            # Handle empty desc_header gracefully
            desc = row.get(desc_header, "").strip() if desc_header else ""

            # Skip empty or metadata-only rows (e.g., "0,0,Means,...")
            if not form or not line or form == "0":
                continue

            db_col = sanitize_col(form, line)
            fields.append((db_col, desc, form, line))
            
    return fields, year_cols, rows


# ── CLI ─────────────────────────────────────────────────────────────────────

DEFAULT_DB = "data/tax_returns.db"
DEFAULT_CSV = os.path.join(os.path.dirname(__file__) or ".", "data", "tax_return", "returns.csv")


@click.group()
def cli():
    """Tax Return Parser — Import tax return data from CSV into SQLite."""
    pass


@cli.command("import")
@click.option("--file", "filepath", type=click.Path(exists=True), default=DEFAULT_CSV,
              show_default=True, help="CSV file export from Google Sheets")
@click.option("--db", default=DEFAULT_DB, show_default=True, help="SQLite database path")
def import_cmd(filepath, db):
    """Import tax return data from CSV into SQLite."""
    try:
        fields, year_cols, rows = extract_schema_from_csv(filepath)
    except Exception as e:
        raise click.ClickException(f"Failed to read CSV: {e}")

    if not year_cols:
        click.echo("⚠ No year columns (e.g., '2023') found in the CSV. Schema initialized, but no data imported.")
        click.echo("  Add columns titled with a 4-digit year to your CSV to import data.")
        init_db(db, fields)
        return

    # Initialize / update the schema dynamically
    init_db(db, fields)

    db_cols = [f[0] for f in fields]
    imported = 0

    # Build a set of valid db_cols for quick lookup
    field_col_set = set(db_cols)

    for year_str in year_cols:
        year = int(year_str.strip())
        data = {}
        for row in rows:
            form = row.get("Form", "").strip()
            line = row.get("Line", "").strip()
            if not form or not line or form == "0":
                continue
            db_col = sanitize_col(form, line)
            if db_col in field_col_set:
                data[db_col] = parse_value(row.get(year_str, ""))

        upsert_return(db, year, data, db_cols)
        click.echo(f"✓ Imported data for {year}")
        imported += 1

    click.echo(f"\nDone: {imported} year(s) imported/updated in dynamic SQLite schema.")




@cli.command()
@click.option("--file", "filepath", type=click.Path(exists=True), default=DEFAULT_CSV,
              help="CSV file to load labels from")
@click.option("--db", default=DEFAULT_DB, show_default=True, help="SQLite database path")
def analyze(db, filepath):
    """Show YoY analysis and derived tax metrics for all tax years."""
    returns = get_all_returns(db)
    if not returns:
        click.echo("No data in database.")
        return

    fields, _, _ = extract_schema_from_csv(filepath)
    analyses, derived_metrics = build_analysis(returns, fields)
    store_analysis(db, analyses, fields)

    click.echo("\n── Derived Metrics By Year ──")
    for analysis in analyses:
        year = analysis["tax_year"]
        metrics = "  ".join(
            f"{label}: {format_metric(analysis[key])}"
            for key, label in derived_metrics
        )
        click.echo(f"  {year}  {metrics}")

    click.echo("\n── YoY Change (%) By Field ──")
    year_labels = [str(a["tax_year"]) for a in analyses]
    click.echo(f"  {'Field':<45s}  {'  '.join(f'{y:>8s}' for y in year_labels)}")
    for db_col, desc, form, line in fields:
        label = f"[{form}:{line}] {desc}"
        vals = "  ".join(
            f"{format_metric(a['yoy_changes_pct'].get(db_col)):>8s}"
            for a in analyses
        )
        click.echo(f"  {label:<45s}  {vals}")
    click.echo(f"\nSaved {len(analyses)} analysis row(s) to tax_return_analysis.")


if __name__ == "__main__":
    cli()
