#!/usr/bin/env python3
"""
Parse Wealthfront bank statement PDFs and import transactions into SQLite.

Usage:
    uv run statement_parser.py data/example.pdf
    uv run statement_parser.py data/*.pdf --db data/transactions.db
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path

import pdfplumber


DEFAULT_DB_PATH = "data/transactions.db"

# Matches lines like: 12/29/2025 AMEX EPAYMENT-ACH PMT-A9912 - $1,023.52
TRANSACTION_RE = re.compile(
    r"^(?P<date>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<description>.+?)\s+"
    r"(?P<sign>[+-])\s+\$(?P<amount>[\d,]+\.\d{2})$"
)

# Matches the subtype line that follows each transaction: Debit, Deposit, etc.
SUBTYPE_RE = re.compile(r"^(Debit|Deposit|Credit|Transfer|Interest|Fee|Withdrawal)$")

# Matches the account number from the header
ACCOUNT_RE = re.compile(r"ACCOUNT NUMBER\n.*?(?P<account>[\d-]+)")

# Matches the statement period from the header
PERIOD_RE = re.compile(
    r"STATEMENT PERIOD.*?\n.*?(?P<period>\w+\.\s+\d+,\s+\d{4}\s+to\s+\w+\.\s+\d+,\s+\d{4})"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse Wealthfront bank statement PDFs into a SQLite database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "input_paths",
        nargs="+",
        help="Path(s) to statement PDF file(s).",
    )
    parser.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help="Path to the SQLite database file.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# PDF text extraction and parsing
# ---------------------------------------------------------------------------


def extract_text(path: Path) -> str:
    """Extract all text from a PDF, joining pages."""
    with pdfplumber.open(str(path)) as pdf:
        pages = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
    return "\n".join(pages)


def extract_account_number(text: str) -> str | None:
    """Pull the account number from the statement header."""
    match = ACCOUNT_RE.search(text)
    return match.group("account") if match else None


def normalize_date(date_str: str) -> str:
    """Convert MM/DD/YYYY to YYYY-MM-DD."""
    month, day, year = date_str.split("/")
    return f"{year}-{month}-{day}"


def parse_transactions(
    text: str,
    source_file: str,
    account_id: str | None,
) -> list[dict[str, object]]:
    """Parse transaction lines from extracted PDF text.

    Each transaction spans two lines:
        MM/DD/YYYY DESCRIPTION + $X,XXX.XX
        Subtype
    """
    lines = text.splitlines()
    transactions: list[dict[str, object]] = []
    in_sweep = False

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Track which section we are in
        if line.startswith("SWEEP TRANSACTIONS"):
            in_sweep = True
            i += 1
            continue

        if line == "TRANSACTIONS":
            in_sweep = False
            i += 1
            continue

        match = TRANSACTION_RE.match(line)
        if match:
            sign = match.group("sign")
            amount_str = match.group("amount").replace(",", "")
            amount = float(amount_str) if sign == "+" else -float(amount_str)

            # Look ahead for subtype on the next line
            subtype = None
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                subtype_match = SUBTYPE_RE.match(next_line)
                if subtype_match:
                    subtype = subtype_match.group(1)
                    i += 1  # consume the subtype line

            txn = {
                "date": normalize_date(match.group("date")),
                "description": match.group("description"),
                "amount": amount,
                "subtype": subtype,
                "category": "sweep" if in_sweep else "transaction",
                "account_id": account_id,
                "source_file": source_file,
            }
            transactions.append(txn)

        i += 1

    return transactions


def parse_statement(path: Path) -> list[dict[str, object]]:
    """Parse a single statement PDF into a list of transaction dicts."""
    text = extract_text(path)
    account_id = extract_account_number(text)
    return parse_transactions(text, path.name, account_id)


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
