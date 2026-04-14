#!/usr/bin/env python3
"""
Tax Return Parser — Import tax return data from a CSV spreadsheet into SQLite.

Usage:
    python tax_return_parser.py import --file ./data/tix_table_format.csv
    python tax_return_parser.py list
    python tax_return_parser.py show --year 2024
    python tax_return_parser.py dump
"""

from __future__ import annotations

import os
import re
import sqlite3
import csv
from datetime import datetime, timezone

import click
from tabulate import tabulate


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


def get_return(db_path: str, year: int) -> dict | None:
    """Fetch a single tax return record by year."""
    if not os.path.exists(db_path):
        return None
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM tax_returns WHERE tax_year = ?", (year,)
        ).fetchone()
        return dict(row) if row else None


# ── CSV Parsing ─────────────────────────────────────────────────────────────

def parse_value(val: str) -> float | None:
    """Parse a value from CSV into a float, handling strings with $ and commas."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = val.strip().replace("$", "").replace(",", "")
        if not cleaned or cleaned.lower() == "null" or cleaned == "-":
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
        if "Year" in headers:
            desc_header = "Year" # As provided in tix_table_format.csv
        elif "Description" in headers:
            desc_header = "Description"
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
            
            if not form or not line:
                continue
                
            db_col = sanitize_col(form, line)
            fields.append((db_col, desc, form, line))
            
    return fields, year_cols, rows


# ── CLI ─────────────────────────────────────────────────────────────────────

DEFAULT_DB = "data/tax_returns.db"
DEFAULT_CSV = os.path.join(os.path.dirname(__file__) or ".", "tax_return_format.csv")


@click.group()
def cli():
    """Tax Return Parser — Import and query tax return data from CSV."""
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

    for year_str in year_cols:
        year = int(year_str.strip())
        data = {}
        for i, row in enumerate(rows):
            if i >= len(fields):
                break
            val_str = row.get(year_str, "")
            data[fields[i][0]] = parse_value(val_str)
            
        upsert_return(db, year, data, db_cols)
        click.echo(f"✓ Imported data for {year}")
        imported += 1

    click.echo(f"\nDone: {imported} year(s) imported/updated in dynamic SQLite schema.")


@cli.command("list")
@click.option("--db", default=DEFAULT_DB, show_default=True, help="SQLite database path")
def list_cmd(db):
    """List all tax years in the database."""
    returns = get_all_returns(db)
    if not returns:
        click.echo("No data in database.")
        return

    table = [[r["tax_year"], r["imported_at"][:10]] for r in returns]
    click.echo(tabulate(table, headers=["Year", "Imported"], tablefmt="simple"))


@cli.command()
@click.option("--year", type=int, required=True, help="Tax year to display")
@click.option("--file", "filepath", type=click.Path(exists=True), default=DEFAULT_CSV,
              help="CSV file to load labels from")
@click.option("--db", default=DEFAULT_DB, show_default=True, help="SQLite database path")
def show(year, filepath, db):
    """Show detailed data for a specific tax year."""
    record = get_return(db, year)
    if not record:
        click.echo(f"No data found for year {year} in database.")
        return

    # Read the fields dynamically to get labels
    fields, _, _ = extract_schema_from_csv(filepath)

    click.echo(f"\n── Tax Year {year} ──")
    click.echo(f"Imported: {record['imported_at']}\n")

    table_rows = []
    total_income = None
    total_tax = None
    taxable_income = None

    for db_col, desc, form, line in fields:
        val = record.get(db_col)
        label = f"[{form}:{line}] {desc}"
        formatted = f"{val:>12,.0f}" if val is not None else "         N/A"
        table_rows.append([label, formatted])
        
        # Heuristics for derived metrics if standard naming is somewhat maintained
        if db_col == "f_1040_9":
            total_income = val
        if db_col == "f_1040_24":
            total_tax = val
        if db_col == "f_1040_15":
            taxable_income = val

    click.echo(tabulate(table_rows, headers=["Field", "Value"], tablefmt="simple"))

    # Show derived metrics
    click.echo("\n── Derived Metrics ──")
    has_metrics = False
    
    if total_income is not None and total_tax is not None and total_income > 0:
        effective_rate = (total_tax / total_income) * 100
        click.echo(f"Effective tax rate: {effective_rate:.1f}%")
        has_metrics = True
        
    if taxable_income is not None and total_tax is not None and taxable_income > 0:
        marginal_rate = (total_tax / taxable_income) * 100
        click.echo(f"Tax / taxable income: {marginal_rate:.1f}%")
        has_metrics = True
        
    if not has_metrics:
        click.echo("Derived metrics (tax rates) unavailable. Missing total income/tax rows.")


@cli.command()
@click.option("--file", "filepath", type=click.Path(exists=True), default=DEFAULT_CSV,
              help="CSV file to load labels from")
@click.option("--db", default=DEFAULT_DB, show_default=True, help="SQLite database path")
def dump(db, filepath):
    """Dump all data as a wide table."""
    returns = get_all_returns(db)
    if not returns:
        click.echo("No data in database.")
        return

    fields, _, _ = extract_schema_from_csv(filepath)
    headers = ["Year"] + [f"[{f[2]}:{f[3]}] {f[1]}" for f in fields]
    
    rows = []
    for r in returns:
        row = [r["tax_year"]]
        for db_col, _, _, _ in fields:
            val = r.get(db_col)
            row.append(f"{val:,.0f}" if val is not None else "")
        rows.append(row)

    click.echo(tabulate(rows, headers=headers, tablefmt="simple"))


if __name__ == "__main__":
    cli()
