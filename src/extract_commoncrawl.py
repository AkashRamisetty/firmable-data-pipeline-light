from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "dbname": "firmable_companies",
    "user": "firmable",
    "password": "firmable_password",
    "host": "localhost",
    "port": 5432,
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_commoncrawl_csv(csv_path: Path, crawl_id: str = "CC-MAIN-2025-03"):
    print(f"📂 Loading Common Crawl sample from: {csv_path}")
    df = pd.read_csv(csv_path)

    # Basic sanity checks / cleaning
    df["url"] = df["url"].astype(str).str.strip()
    df["domain"] = df["domain"].astype(str).str.lower().str.strip()
    df["tld"] = df["tld"].astype(str).str.lower().str.strip()

    rows = []
    for _, row in df.iterrows():
        rows.append(
            (
                crawl_id,
                row["url"],
                row["domain"],
                row.get("tld", None),
                row.get("html_title", None),
                row.get("extracted_name", None),
                row.get("extracted_industry", None),
            )
        )

    print(f"✅ Prepared {len(rows)} rows for insertion into raw_commoncrawl")
    if not rows:
        return

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO raw_commoncrawl (
            crawl_id,
            url,
            domain,
            tld,
            html_title,
            extracted_name,
            extracted_industry
        )
        VALUES %s
    """

    execute_values(cur, insert_sql, rows)
    cur.close()
    conn.close()
    print("✅ Inserted Common Crawl sample into raw_commoncrawl.")


def main():
    csv_path = Path(__file__).resolve().parent.parent / "data" / "commoncrawl_sample.csv"
    load_commoncrawl_csv(csv_path)


if __name__ == "__main__":
    main()
