from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "dbname": "firmable_companies",
    "user": "firmable",
    "password": "firmable_password",
    "host": "localhost",
    "port": 5432,
}

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "commoncrawl"
CDX_FILE = DATA_DIR / "cc-index-cdx-00000.gz"
MAX_RECORDS = 100_000
BATCH_SIZE = 2_000
CRAWL_ID = "CC-MAIN-2025-13"


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def extract_host_and_tld(url: str) -> Tuple[str | None, str | None]:
    """
    Parse host and a simple TLD from a URL using stdlib only.
    """
    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if not host:
            return None, None

        # Strip port if present
        if ":" in host:
            host = host.split(":", 1)[0]

        parts = host.split(".")
        if len(parts) >= 3 and parts[-1] == "au":
            tld = ".".join(parts[-2:])  # e.g. com.au, org.au
        elif len(parts) >= 2:
            tld = ".".join(parts[-2:])
        else:
            tld = parts[0]

        return host, tld
    except Exception:
        return None, None


def derive_company_name_from_domain(domain: str) -> str | None:
    """
    Cheap heuristic: derive a 'company name' from the leftmost label
    of the domain.
    """
    if not domain:
        return None

    clean = domain.lower()
    for prefix in ("www.", "m.", "web."):
        if clean.startswith(prefix):
            clean = clean[len(prefix):]

    parts = clean.split(".")
    if len(parts) > 1:
        label = parts[0]
    else:
        label = clean

    name = "".join(ch for ch in label if ch.isalnum())
    if not name:
        return None
    return name.upper()


def normalize_name(name: str | None) -> str | None:
    if not name:
        return None
    return " ".join(name.upper().split())


def stream_cdx_records():
    """
    Stream the CDX file and yield up to MAX_RECORDS records.
    We *prefer* .au domains, but we don't hard-filter on them because
    some shards (like the one we're using) may have 0 .au URLs.
    We still count how many .au hosts we saw for stats.
    """
    if not CDX_FILE.exists():
        raise FileNotFoundError(f"CDX file not found: {CDX_FILE}")

    count_yielded = 0
    count_seen_au = 0

    with gzip.open(CDX_FILE, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split(" ", 2)
            if len(parts) < 3:
                continue

            _, _, json_str = parts
            try:
                meta = json.loads(json_str)
            except Exception:
                continue

            url = meta.get("url")
            if not url:
                continue

            host, tld = extract_host_and_tld(url)
            if not host or not tld:
                continue

            if host.endswith(".au"):
                count_seen_au += 1

            company_name_raw = derive_company_name_from_domain(host)
            company_name_norm = normalize_name(company_name_raw)

            record = {
                "crawl_id": CRAWL_ID,
                "url": url,
                "domain": host,
                "tld": tld,
                "html_title": None,
                "company_name_raw": company_name_raw,
                "company_name_norm": company_name_norm,
                "industry": None,
                "fetched_at": datetime.utcnow(),
            }

            yield record
            count_yielded += 1

            if count_yielded >= MAX_RECORDS:
                break

    print(f"🔍 CDX scan complete. .au URLs seen: {count_seen_au}, yielded: {count_yielded}")


def load_commoncrawl_into_db():
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("Using CC index file:", CDX_FILE)
        print("🧹 Truncating raw_commoncrawl before load (idempotent demo)...")
        cur.execute("TRUNCATE TABLE raw_commoncrawl;")

        insert_sql = """
            INSERT INTO raw_commoncrawl (
                crawl_id,
                url,
                domain,
                tld,
                html_title,
                company_name_raw,
                company_name_norm,
                industry,
                fetched_at
            )
            VALUES %s
        """

        batch: List[tuple] = []
        total = 0

        for rec in stream_cdx_records():
            batch.append(
                (
                    rec["crawl_id"],
                    rec["url"],
                    rec["domain"],
                    rec["tld"],
                    rec["html_title"],
                    rec["company_name_raw"],
                    rec["company_name_norm"],
                    rec["industry"],
                    rec["fetched_at"],
                )
            )

            if len(batch) >= BATCH_SIZE:
                execute_values(cur, insert_sql, batch)
                total += len(batch)
                print(f"    ✅ Inserted {total} rows so far...")
                batch.clear()

        if batch:
            execute_values(cur, insert_sql, batch)
            total += len(batch)
            batch.clear()

        conn.commit()
        print(f"🎉 Common Crawl load complete. Total rows inserted: {total}")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error during Common Crawl load: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_commoncrawl_into_db()
