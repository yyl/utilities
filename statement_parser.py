#!/usr/bin/env python3
"""
Parse bank statement CSVs and import transactions into SQLite.

Supports:
  - Wealthfront: columns Transaction date, Description, Type, Amount
  - Discover:    columns Transaction Date, Transaction Description,
                 Transaction Type, Debit, Credit, Balance

When multiple sources are imported together, inter-account transfers
(same date, matching absolute amount, opposite signs) are automatically
detected and excluded to prevent double-counting.

Usage:
    uv run statement_parser.py data/wf.csv
    uv run statement_parser.py data/wf.csv data/discover.csv
    uv run statement_parser.py data/statements/*.csv --db data/transactions.db
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
        description="Parse bank statement CSVs into a SQLite database.",
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
# CSV format detection
# ---------------------------------------------------------------------------


# Canonical header sets used for auto-detection.
_WF_HEADERS = {"transaction date", "description", "type", "amount"}
_DISCOVER_HEADERS = {
    "transaction date",
    "transaction description",
    "transaction type",
    "debit",
    "credit",
    "balance",
}


def detect_format(path: Path) -> str:
    """Return 'wealthfront', 'discover', or raise on unknown format."""
    with path.open(newline="", encoding="utf-8") as f:
        header = f.readline().strip()
    cols = {c.strip().lower() for c in header.split(",")}
    if _DISCOVER_HEADERS.issubset(cols):
        return "discover"
    if _WF_HEADERS.issubset(cols):
        return "wealthfront"
    raise ValueError(
        f"Unrecognised CSV header in {path.name}: {header}"
    )


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


def _parse_dollar(value: str) -> float:
    """Parse a dollar string like '$1,600.00' or '0' into a float."""
    cleaned = value.strip().replace("$", "").replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_wealthfront(path: Path) -> list[dict[str, object]]:
    """Parse a Wealthfront statement CSV."""
    transactions: list[dict[str, object]] = []
    with path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            date_str = row.get("Transaction date") or row.get("date") or ""
            description = row.get("Description") or row.get("description") or ""
            subtype = row.get("Type") or row.get("type") or ""
            amount_str = row.get("Amount") or row.get("amount") or "0"

            try:
                amount = float(amount_str.replace(",", ""))
            except ValueError:
                amount = 0.0

            transactions.append({
                "date": normalize_date(date_str),
                "description": description,
                "amount": amount,
                "subtype": subtype,
                "category": "transaction",
                "account_id": "wealthfront",
                "source_file": path.name,
            })
    return transactions


def parse_discover(path: Path) -> list[dict[str, object]]:
    """Parse a Discover statement CSV (Debit / Credit / Balance columns)."""
    transactions: list[dict[str, object]] = []
    with path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            date_str = (
                row.get("Transaction Date") or row.get("transaction date") or ""
            )
            description = (
                row.get("Transaction Description")
                or row.get("transaction description")
                or ""
            )
            txn_type = (
                row.get("Transaction Type")
                or row.get("transaction type")
                or ""
            )
            debit = _parse_dollar(row.get("Debit") or row.get("debit") or "0")
            credit = _parse_dollar(
                row.get("Credit") or row.get("credit") or "0"
            )

            # Discover lists debits as positive numbers in the Debit column.
            # Unified convention: negative = money leaving the account.
            amount = credit - debit

            transactions.append({
                "date": normalize_date(date_str),
                "description": description,
                "amount": amount,
                "subtype": txn_type,
                "category": "transaction",
                "account_id": "discover",
                "source_file": path.name,
            })
    return transactions


def parse_statement(path: Path) -> list[dict[str, object]]:
    """Auto-detect format and parse a statement CSV."""
    fmt = detect_format(path)
    if fmt == "discover":
        return parse_discover(path)
    return parse_wealthfront(path)


# ---------------------------------------------------------------------------
# Inter-account transfer cancellation
# ---------------------------------------------------------------------------


def cancel_transfers(
    transactions: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Remove pairs of transactions that represent the same inter-account
    transfer (same date, matching absolute amount, opposite signs, different
    accounts).

    Returns a new list with matched pairs removed.
    """
    # Group by (date, abs_amount) to find candidate pairs.
    from collections import defaultdict

    BucketKey = tuple  # (date, abs_amount)
    buckets: dict[BucketKey, list[int]] = defaultdict(list)
    for idx, txn in enumerate(transactions):
        key = (txn["date"], round(abs(txn["amount"]), 2))  # type: ignore[arg-type]
        buckets[key].append(idx)

    cancelled: set[int] = set()
    for key, indices in buckets.items():
        if len(indices) < 2:
            continue
        # Separate positives and negatives from *different* accounts.
        positives = [
            i for i in indices if transactions[i]["amount"] > 0  # type: ignore[operator]
        ]
        negatives = [
            i for i in indices if transactions[i]["amount"] < 0  # type: ignore[operator]
        ]
        # Greedily pair one positive with one negative from a different account.
        used_neg: set[int] = set()
        for pi in positives:
            for ni in negatives:
                if ni in used_neg:
                    continue
                if transactions[pi]["account_id"] != transactions[ni]["account_id"]:
                    cancelled.add(pi)
                    cancelled.add(ni)
                    used_neg.add(ni)
                    break

    kept = [t for i, t in enumerate(transactions) if i not in cancelled]
    n = len(cancelled)
    if n:
        print(
            f"  ↳ Cancelled {n} transactions ({n // 2} inter-account "
            f"transfer pair(s))"
        )
    return kept


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
    all_transactions: list[dict[str, object]] = []

    # Phase 1: Parse all files.
    for input_str in args.input_paths:
        input_path = Path(input_str)
        if not input_path.exists():
            print(f"File not found: {input_path}", file=sys.stderr)
            continue

        try:
            fmt = detect_format(input_path)
            txns = parse_statement(input_path)
        except Exception as exc:
            print(f"Failed to parse {input_path}: {exc}", file=sys.stderr)
            continue

        if not txns:
            print(f"No transactions found in {input_path.name}")
            continue

        print(f"{input_path.name}: {len(txns)} transactions parsed ({fmt})")
        all_transactions.extend(txns)

    if not all_transactions:
        print("No transactions to import.")
        return

    # Phase 2: Cancel inter-account transfers when multiple sources present.
    account_ids = {t["account_id"] for t in all_transactions}
    if len(account_ids) > 1:
        all_transactions = cancel_transfers(all_transactions)

    # Phase 3: Write to SQLite.
    db_path = Path(args.db)
    inserted, skipped = write_to_sqlite(all_transactions, db_path)
    print(
        f"\nTotal: {inserted} imported, {skipped} skipped → {args.db}"
    )


if __name__ == "__main__":
    main()
