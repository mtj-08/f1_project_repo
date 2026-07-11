"""
validate_notebooks.py

Lightweight CI gate for the F1 Lakehouse project notebooks.

Checks, for every .ipynb under f1_project_batch/ and f1_project_incremental_batch/:
  1. The file is valid JSON / valid Jupyter notebook format (nbformat).
  2. No cell contains an obvious hardcoded secret (token/password/connection string).
  3. No cell contains a hardcoded ABFSS storage account key or SAS token pattern.

Exit code 0 = pass, 1 = fail (fails the GitHub Actions job).
"""

import json
import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
TARGET_DIRS = ["f1_project_batch", "f1_project_incremental_batch"]

SECRET_PATTERNS = [
    re.compile(r"(?i)(?:api_key|apikey|password|secret)\s*=\s*['\"][^'\"]{4,}['\"]"),
    re.compile(r"(?i)dbutils\.secrets\.get\([^)]*\)\s*==\s*['\"]"),  # comparing a secret to a literal
    re.compile(r"AccountKey=[A-Za-z0-9+/=]{20,}"),
    re.compile(r"sig=[A-Za-z0-9%]{20,}"),  # SAS token signature
    re.compile(r"(?i)token\s*=\s*['\"][A-Za-z0-9_\-]{20,}['\"]"),
]


def find_notebooks() -> list[pathlib.Path]:
    notebooks = []
    for d in TARGET_DIRS:
        base = REPO_ROOT / d
        if base.exists():
            notebooks.extend(sorted(base.rglob("*.ipynb")))
    return notebooks


def validate_notebook(path: pathlib.Path) -> list[str]:
    errors = []
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception as e:
        return [f"Could not read file: {e}"]

    try:
        nb = json.loads(raw)
    except json.JSONDecodeError as e:
        return [f"Invalid JSON / corrupted notebook: {e}"]

    if "cells" not in nb:
        errors.append("Missing 'cells' key — not a valid notebook structure.")
        return errors

    for i, cell in enumerate(nb.get("cells", [])):
        source = "".join(cell.get("source", []))
        for pattern in SECRET_PATTERNS:
            if pattern.search(source):
                errors.append(
                    f"Cell {i}: possible hardcoded secret/credential matching pattern '{pattern.pattern}'"
                )

    return errors


def main() -> int:
    notebooks = find_notebooks()
    if not notebooks:
        print("No notebooks found under f1_project_batch/ or f1_project_incremental_batch/ — nothing to validate.")
        return 0

    print(f"Validating {len(notebooks)} notebook(s)...\n")
    failed = False

    for nb_path in notebooks:
        rel = nb_path.relative_to(REPO_ROOT)
        errors = validate_notebook(nb_path)
        if errors:
            failed = True
            print(f"❌ {rel}")
            for err in errors:
                print(f"   - {err}")
        else:
            print(f"✅ {rel}")

    print()
    if failed:
        print("Validation FAILED. Fix the issues above before merging.")
        return 1

    print("All notebooks passed validation.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
