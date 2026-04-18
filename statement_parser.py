#!/usr/bin/env python3
"""
Parse Wealthfront bank statement CSVs and import transactions into SQLite.

Usage:
    uv run statement_parser.py data/wf.csv
    uv run statement_parser.py data/*.csv --db data/transactions.db
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path


DEFAULT_DB_PATH = "data/transactions.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse Wealthfront bank statement CSVs into a SQLite database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "input_paths",
        nargs="+",
        help="Path(s) to statement CSV file(s).",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help="Path to the SQLite database file.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


def normalize_date(date_str: str) -> str:
    """Convert MM/DD/YYYY to YYYY-MM-DD."""
    try:
        month, day, year = date_str.split("/")
        return f"{year}-{month:0>2}-{day:0>2}"
    except ValueError:
        return date_str


def parse_statement(path: Path) -> list[dict[str, object]]:
    """Parse a single statement CSV into a list of transaction dicts."""
    transactions: list[dict[str, object]] = []
    
    with path.open(newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Expected columns: Transaction date,Description,Type,Amount
            date_str = row.get("Transaction date") or row.get("date") or ""
            description = row.get("Description") or row.get("description") or ""
            subtype = row.get("Type") or row.get("type") or ""
            amount_str = row.get("Amount") or row.get("amount") or "0"
            
            try:
                amount = float(amount_str.replace(",", ""))
            except ValueError:
                amount = 0.0

            txn = {
                "date": normalize_date(date_str),
                "description": description,
                "amount": amount,
                "subtype": subtype,
                "category": "transaction",  # Default category for CSV imports
                "account_id": "",           # CSV doesn't specify account_id
                "source_file": path.name,
            }
            transactions.append(txn)
            
    return transactions


# ---------------------------------------------------------------------------
# SQLite output
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """\
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
"""

INSERT_SQL = """\
INSERT OR IGNORE INTO transactions
    (date, description, amount, subtype, category, account_id, source_file)
VALUES
    (:date, :description, :amount, :subtype, :category, :account_id, :source_file);
"""


def write_to_sqlite(
    transactions: list[dict[str, object]],
    db_path: Path,
) -> tuple[int, int]:
    """Write transactions to SQLite. Returns (inserted, skipped) counts."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(CREATE_TABLE_SQL)
        inserted = 0
        for txn in transactions:
            cursor = conn.execute(INSERT_SQL, txn)
            if cursor.rowcount > 0:
                inserted += 1
        conn.commit()
    finally:
        conn.close()

    skipped = len(transactions) - inserted
    return inserted, skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    total_inserted = 0
    total_skipped = 0

    for input_str in args.input_paths:
        input_path = Path(input_str)
        if not input_path.exists():
            print(f"File not found: {input_path}", file=sys.stderr)
            continue

        try:
            transactions = parse_statement(input_path)
        except Exception as exc:
            print(f"Failed to parse {input_path}: {exc}", file=sys.stderr)
            continue

        if not transactions:
            print(f"No transactions found in {input_path.name}")
            continue

        db_path = Path(args.db)
        inserted, skipped = write_to_sqlite(transactions, db_path)
        total_inserted += inserted
        total_skipped += skipped
        print(
            f"{input_path.name}: {inserted} imported, {skipped} skipped"
        )

    if len(args.input_paths) > 1:
        print(
            f"\nTotal: {total_inserted} imported, "
            f"{total_skipped} skipped → {args.db}"
        )
    elif total_inserted or total_skipped:
        print(f"Database: {args.db}")


if __name__ == "__main__":
    main()
