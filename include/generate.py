"""
Synthetic UK-retail order generator.
Schema modelled on UCI Online Retail II dataset.
Produces approximately 15% faulty rows to exercise dbt tests downstream.
"""
from __future__ import annotations

import csv
import os
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

COUNTRIES = [
    "United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom",
    "United Kingdom", "United Kingdom", "United Kingdom",
    "Germany", "France", "Netherlands", "Belgium", "Spain", "Australia",
    "Switzerland", "Portugal", "Norway", "Sweden", "Denmark", "Finland",
    "United States", "Japan", "Singapore",
]

PRODUCTS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.55),
    ("71053",  "WHITE METAL LANTERN", 3.39),
    ("84406B", "CREAM CUPID HEARTS COAT HANGER", 2.75),
    ("84029G", "KNITTED UNION FLAG HOT WATER BOTTLE", 3.39),
    ("84029E", "RED WOOLLY HOTTIE WHITE HEART", 3.39),
    ("22752",  "SET 7 BABUSHKA NESTING BOXES", 7.65),
    ("21730",  "GLASS STAR FROSTED T-LIGHT HOLDER", 4.25),
    ("22633",  "HAND WARMER UNION JACK", 1.85),
    ("22632",  "HAND WARMER RED POLKA DOT", 1.85),
    ("47566",  "PARTY BUNTING", 4.95),
    ("85099B", "JUMBO BAG RED RETROSPOT", 1.65),
    ("22745",  "POPPY'S PLAYHOUSE BEDROOM", 2.10),
    ("22748",  "POPPY'S PLAYHOUSE KITCHEN", 2.10),
    ("22749",  "FELTCRAFT PRINCESS CHARLOTTE DOLL", 3.75),
    ("22310",  "IVORY KNITTED MUG COSY", 1.65),
    ("84969",  "BOX OF 6 ASSORTED COLOUR TEASPOONS", 4.25),
    ("22623",  "BOX OF VINTAGE JIGSAW BLOCKS", 4.95),
    ("22622",  "BOX OF VINTAGE ALPHABET BLOCKS", 9.95),
    ("21754",  "HOME BUILDING BLOCK WORD", 5.95),
    ("21755",  "LOVE BUILDING BLOCK WORD", 5.95),
]


def _random_invoice_no(rng: random.Random) -> str:
    return str(rng.randint(536000, 581000))


def _random_customer_id(rng: random.Random) -> str | None:
    if rng.random() < 0.05:
        return None
    return str(rng.randint(12346, 18287))


def generate_orders(run_date: str, output_dir: str | None = None, seed: int | None = None) -> str:
    """
    Generate a batch of synthetic orders for *run_date* (YYYY-MM-DD).

    Returns the path to the written CSV file.
    Fault injection summary (approximately 15% of rows):
      - 5% missing CustomerID
      - 5% negative Quantity (returns)
      - 3% zero or negative Price
      - 2% duplicate invoice lines
    """
    rng = random.Random(seed or hash(run_date) & 0xFFFFFFFF)

    date = datetime.strptime(run_date, "%Y-%m-%d")

    if output_dir is None:
        output_dir = os.path.join(os.environ.get("CADENCE_DATA_DIR", "/tmp/cadence"), run_date)

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_path = os.path.join(output_dir, "orders.csv")

    row_count = rng.randint(800, 1400)
    rows: List[Dict[str, Any]] = []

    for _ in range(row_count):
        invoice_no = _random_invoice_no(rng)
        stock_code, description, base_price = rng.choice(PRODUCTS)
        quantity = rng.randint(1, 24)
        invoice_minute = rng.randint(0, 1439)
        invoice_date = date + timedelta(minutes=invoice_minute)
        price = round(base_price * rng.uniform(0.9, 1.1), 2)
        customer_id = _random_customer_id(rng)
        country = rng.choice(COUNTRIES)

        fault = rng.random()
        if fault < 0.05:
            quantity = -abs(quantity)
        elif fault < 0.08:
            price = round(-abs(price), 2)
        elif fault < 0.10:
            price = 0.0

        rows.append({
            "invoice_no": invoice_no,
            "stock_code": stock_code,
            "description": description,
            "quantity": quantity,
            "invoice_date": invoice_date.strftime("%Y-%m-%d %H:%M:%S"),
            "price": price,
            "customer_id": customer_id,
            "country": country,
            "run_date": run_date,
        })

    duplicate_pool = [r for r in rows[:50]]
    for row in rng.sample(duplicate_pool, min(len(duplicate_pool), int(row_count * 0.02))):
        rows.append(row)

    rng.shuffle(rows)

    fieldnames = ["invoice_no", "stock_code", "description", "quantity",
                  "invoice_date", "price", "customer_id", "country", "run_date"]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return output_path
