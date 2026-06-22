"""
Smoke test: runs generate -> load -> dbt_run -> dbt_test locally without Airflow.
Use this to verify the pipeline logic before standing up the full stack.

  python scripts/smoke_test.py
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / "include"))

DB_PATH = str(ROOT / "data" / "cadence.duckdb")
DATA_DIR = str(ROOT / "data" / "staging")
TRANSFORM_DIR = str(ROOT / "transform")

os.environ["CADENCE_DB_PATH"] = DB_PATH
os.environ["CADENCE_DATA_DIR"] = DATA_DIR


def step(label: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print("=" * 60)


def main() -> None:
    from generate import generate_orders
    from load import load_orders

    run_date = "2025-01-15"

    step("1. Generate orders")
    csv_path = generate_orders(run_date=run_date, output_dir=os.path.join(DATA_DIR, run_date))
    print(f"CSV written to: {csv_path}")
    with open(csv_path) as f:
        lines = f.readlines()
    print(f"Rows generated (including header): {len(lines)}")
    assert len(lines) > 1, "Generator produced no rows"

    step("2. Load to DuckDB (first load)")
    count1 = load_orders(run_date=run_date, csv_path=csv_path)
    print(f"Rows after first load: {count1}")
    assert count1 > 0, "Load produced no rows"

    step("3. Load to DuckDB (second load — idempotency check)")
    count2 = load_orders(run_date=run_date, csv_path=csv_path)
    print(f"Rows after second load: {count2}")
    assert count1 == count2, f"Idempotency failure: {count1} != {count2}"
    print("Idempotency OK: row count unchanged on re-run.")

    step("4. dbt run")
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", ".", "--target", "dev", "--no-use-colors"],
        cwd=TRANSFORM_DIR,
        env={**os.environ},
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(f"dbt run failed (exit {result.returncode})")

    step("5. dbt test")
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", ".", "--target", "dev", "--no-use-colors"],
        cwd=TRANSFORM_DIR,
        env={**os.environ},
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        sys.exit(f"dbt test failed (exit {result.returncode})")

    step("All steps passed")
    print(f"Database: {DB_PATH}")
    print(f"Run date: {run_date}")
    print(f"Rows loaded: {count2}")


if __name__ == "__main__":
    main()
