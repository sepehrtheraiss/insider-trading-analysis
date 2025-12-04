#!/usr/bin/env python3

"""
Automatically replace old camelCase Form-4 field names
with the new snake_case column names across ALL .py files.
"""

import os
import re
from pathlib import Path

# ---------------------------------------------------------
# 1. FULL EXACT FIELD MAP (from your schema)
# ---------------------------------------------------------
REPLACEMENTS = {
    "filedAt": "filed_at",
    "periodOfReport": "period_of_report",
    "documentType": "document_type",
    "issuerTicker": "issuer_ticker",
    "issuerCik": "issuer_cik",
    "issuerName": "issuer_name",
    "reporter": "reporter",
    "reporterCik": "reporter_cik",
    "isOfficer": "is_officer",
    "officerTitle": "officer_title",
    "isDirector": "is_director",
    "isTenPercentOwner": "is_ten_percent_owner",
    "table": "table",
    "code": "code",
    "acquiredDisposed": "acquired_disposed",
    "transactionDate": "transaction_date",
    "shares": "shares",
    "pricePerShare": "price_per_share",
    "totalValue": "total_value",
    "sharesOwnedFollowing": "shares_owned_following",
    "is10b5_1": "is_10b5_1",
    "name": "name",
    "cik": "cik",
    "exchange": "exchange",
    "isDelisted": "is_delisted",
    "category": "category",
    "sector": "sector",
    "industry": "industry",
    "sicSector": "sic_sector",
    "sicIndustry": "sic_industry",
}

# ---------------------------------------------------------
# 2. SAFE EXACT-MATCH regex for replacements
# ---------------------------------------------------------
# We use word boundaries \b so we don't replace inside other words.
REGEX_MAP = {re.compile(rf"\b{k}\b"): v for k, v in REPLACEMENTS.items()}


# ---------------------------------------------------------
# 3. Walk all .py files and replace text
# ---------------------------------------------------------
def process_file(path: Path):
    original = path.read_text()
    updated = original
    changes = []

    for pattern, replacement in REGEX_MAP.items():
        new_text, count = pattern.subn(replacement, updated)
        if count > 0:
            updated = new_text
            changes.append((pattern.pattern, replacement, count))

    if changes:
        path.write_text(updated)
        print(f"Updated {path}:")
        for pattern, replacement, count in changes:
            print(f"  {count} × '{pattern}' → '{replacement}'")


def main():
    ROOT = Path(".")
    print("Scanning project for camelCase fields...")

    for path in ROOT.rglob("*.py"):
        if "env/" in str(path) or "venv/" in str(path):
            continue

        process_file(path)

    print("Done.")


if __name__ == "__main__":
    main()

