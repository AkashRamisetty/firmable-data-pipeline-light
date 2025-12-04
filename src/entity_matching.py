from typing import List, Dict, Tuple
from datetime import datetime
from pathlib import Path
import os
import json

import psycopg2
import pandas as pd
from rapidfuzz import fuzz
from psycopg2.extras import execute_values

# OpenAI client (uses OPENAI_API_KEY from environment)
try:
    from openai import OpenAI
    openai_client = OpenAI()
except Exception:
    openai_client = None


DB_CONFIG = {
    "dbname": "firmable_companies",
    "user": "firmable",
    "password": "firmable_password",
    "host": "localhost",
    "port": 5432,
}

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
LLM_MAX_REVIEWS = 10
LLM_LOG_PATH = Path("data/llm_match_logs.jsonl")


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_staging_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load staging tables into pandas DataFrames (SAMPLED for demo).
    Full volumes are already ingested in Postgres:
      - ~19.7M ABR rows  (stg_abr_entities)
      - 100k CC rows     (stg_commoncrawl_companies)

    Here we sample:
      - ABR: active entities, non-null state, ~1/5000 by ABN
      - CC:  ~1/20 by commoncrawl_id
    """
    conn = get_connection()

    abr_df = pd.read_sql_query(
        """
        SELECT
            abn,
            entity_name_norm,
            entity_name_raw,
            entity_type,
            entity_status,
            address_full,
            suburb,
            postcode,
            state,
            start_date_raw
        FROM stg_abr_entities
        WHERE state IS NOT NULL
          AND entity_status = 'ACT'
          AND (abn::bigint % 5000) = 0
        """,
        conn,
    )

    cc_df = pd.read_sql_query(
        """
        SELECT
            commoncrawl_id,
            crawl_id,
            url,
            domain,
            tld,
            html_title,
            company_name_raw,
            company_name_norm,
            industry,
            fetched_at
        FROM stg_commoncrawl_companies
        WHERE (commoncrawl_id % 20) = 0
        """,
        conn,
    )

    conn.close()

    print(
        f"📊 Loaded {len(abr_df)} ABR entities and {len(cc_df)} Common Crawl companies (sampled)."
    )
    return abr_df, cc_df


def parse_date_safe(date_str: str):
    if not date_str:
        return None
    try:
        s = str(date_str)
        if len(s) == 8 and s.isdigit():
            return datetime.strptime(s, "%Y%m%d").date()
        elif len(s) == 10 and "-" in s:
            return datetime.strptime(s, "%Y-%m-%d").date()
        else:
            return None
    except Exception:
        return None


def fuzzy_match_entities(
    abr_df: pd.DataFrame,
    cc_df: pd.DataFrame,
    high_threshold: int = 90,
    low_threshold: int = 75,
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    For this assignment demo, we don't rely on strict thresholds.

    We:
      - Find the best ABR candidate for each CC row using fuzzy token_sort_ratio
      - Treat ALL best pairs as "ambiguous_matches" to be reviewed by the LLM
      - Leave high_conf_matches empty (we're focusing on the LLM-assisted part)

    Returns:
      - high_conf_matches: [] (empty for this demo)
      - ambiguous_matches: list[dict] for all CC rows that have some ABR candidate
      - unmatched_cc: list of CC rows with no ABR candidate (edge cases)
    """
    high_conf_matches: List[Dict] = []
    ambiguous_matches: List[Dict] = []
    unmatched_cc: List[Dict] = []

    if abr_df.empty or cc_df.empty:
        return high_conf_matches, ambiguous_matches, unmatched_cc

    abr_df = abr_df.fillna("")
    cc_df = cc_df.fillna("")

    print("🔎 Starting fuzzy matching on sampled data...")

    for _, cc_row in cc_df.iterrows():
        cc_name = (cc_row.get("company_name_norm") or "").strip()
        if not cc_name:
            unmatched_cc.append(cc_row.to_dict())
            continue

        best_score = -1
        best_abr_row = None

        for _, abr_row in abr_df.iterrows():
            abr_name = (abr_row.get("entity_name_norm") or "").strip()
            if not abr_name:
                continue
            score = fuzz.token_sort_ratio(cc_name, abr_name)
            if score > best_score:
                best_score = score
                best_abr_row = abr_row

        if best_abr_row is not None:
            # For the purposes of the LLM demo, treat everything as ambiguous
            ambiguous_matches.append(
                {
                    "cc": cc_row.to_dict(),
                    "abr": best_abr_row.to_dict(),
                    "score": best_score,
                    "method": "fuzzy_name_ambiguous",
                }
            )
        else:
            unmatched_cc.append(cc_row.to_dict())

    print(
        f"✅ Fuzzy matching complete. "
        f"High-confidence: {len(high_conf_matches)}, "
        f"Ambiguous: {len(ambiguous_matches)}, "
        f"Unmatched CC rows: {len(unmatched_cc)}"
    )
    return high_conf_matches, ambiguous_matches, unmatched_cc


def build_llm_prompt(amb_match: Dict) -> str:
    """
    Build a compact prompt for LLM disambiguation of one CC–ABR pair.
    """
    cc = amb_match["cc"]
    abr = amb_match["abr"]

    cc_name = cc.get("company_name_norm") or cc.get("company_name_raw")
    cc_url = cc.get("url")
    cc_domain = cc.get("domain")

    abr_abn = abr.get("abn")
    abr_name_norm = abr.get("entity_name_norm")
    abr_name_raw = abr.get("entity_name_raw")
    abr_type = abr.get("entity_type")
    abr_status = abr.get("entity_status")
    abr_addr = abr.get("address_full")
    abr_suburb = abr.get("suburb")
    abr_state = abr.get("state")
    abr_postcode = abr.get("postcode")

    prompt = f"""
You are matching Australian companies between a website (Common Crawl) and an ABR record.

Common Crawl company:
- Normalised name: {cc_name}
- URL: {cc_url}
- Domain: {cc_domain}

ABR candidate:
- ABN: {abr_abn}
- Entity name (normalised): {abr_name_norm}
- Entity name (raw): {abr_name_raw}
- Entity type: {abr_type}
- Status: {abr_status}
- Address: {abr_addr}, {abr_suburb}, {abr_state} {abr_postcode}

Question:
Are these records referring to the same underlying company?

Respond **only** with a JSON object with the following shape:
{{
  "is_match": true or false,
  "confidence": "low" | "medium" | "high",
  "reason": "short explanation here"
}}
"""
    return prompt.strip()


def llm_review_ambiguous(
    ambiguous_matches: List[Dict],
    max_to_review: int = LLM_MAX_REVIEWS,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Use an LLM to review a small number of ambiguous matches.

    Returns:
      - llm_approved_matches: list of matches we accept based on LLM decision
      - remaining_ambiguous: list of matches not approved by LLM
    """
    if not ambiguous_matches:
        print("ℹ️ No ambiguous matches to send to LLM.")
        return [], []

    if openai_client is None or os.getenv("OPENAI_API_KEY") is None:
        print("💡 OPENAI_API_KEY not set or client unavailable – skipping LLM review.")
        return [], ambiguous_matches

    to_review = ambiguous_matches[:max_to_review]
    remaining = ambiguous_matches[max_to_review:]

    llm_approved: List[Dict] = []

    # Ensure log file directory exists
    if not LLM_LOG_PATH.parent.exists():
        LLM_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"🤖 Sending {len(to_review)} ambiguous matches to LLM ({OPENAI_MODEL}) for review...")

    for amb in to_review:
        prompt = build_llm_prompt(amb)

        try:
            response = openai_client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an assistant that matches Australian companies "
                            "between website data and ABR records. Respond ONLY with JSON."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
            )

            content = response.choices[0].message.content.strip()

            # Log prompt + response for inspection in README
            with LLM_LOG_PATH.open("a", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {
                            "prompt": prompt,
                            "response": content,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )

            # Parse JSON output
            decision = json.loads(content)
            is_match = bool(decision.get("is_match"))
            confidence = str(decision.get("confidence", "")).lower()

            if is_match and confidence in ("high", "medium"):
                amb_copy = dict(amb)
                amb_copy["method"] = "llm_disambiguation"
                llm_approved.append(amb_copy)

        except Exception as e:
            print(f"⚠️ LLM call failed for one match: {e}")

    print(
        f"🤖 LLM review complete. "
        f"Approved: {len(llm_approved)}, "
        f"Remaining ambiguous: {len(remaining) + (len(to_review) - len(llm_approved))}"
    )

    still_ambiguous = [m for m in to_review if m not in llm_approved] + remaining
    return llm_approved, still_ambiguous


def write_matches_to_db(matches: List[Dict]):
    """
    Insert matches into company_unified and company_source_link.
    """
    if not matches:
        print("⚠️ No matches to write.")
        return

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("🧹 Truncating existing unified company data (for demo idempotency)...")
        cur.execute("TRUNCATE TABLE company_source_link, company_unified RESTART IDENTITY CASCADE;")

        insert_unified_sql = """
            INSERT INTO company_unified (
                abn,
                unified_name,
                unified_name_norm,
                website_domain,
                website_url_sample,
                industry,
                entity_type,
                entity_status,
                address_full,
                suburb,
                postcode,
                state,
                start_date,
                match_confidence,
                match_method
            )
            VALUES (
                %(abn)s,
                %(unified_name)s,
                %(unified_name_norm)s,
                %(website_domain)s,
                %(website_url_sample)s,
                %(industry)s,
                %(entity_type)s,
                %(entity_status)s,
                %(address_full)s,
                %(suburb)s,
                %(postcode)s,
                %(state)s,
                %(start_date)s,
                %(match_confidence)s,
                %(match_method)s
            )
            RETURNING company_id
        """

        insert_link_sql = """
            INSERT INTO company_source_link (
                company_id,
                source_system,
                source_key
            )
            VALUES (%s, %s, %s)
        """

        inserted_count = 0

        for m in matches:
            cc = m["cc"]
            abr = m["abr"]
            score = m["score"]
            method = m["method"]

            start_date = parse_date_safe(abr.get("start_date_raw", ""))

            unified_record = {
                "abn": abr["abn"],
                "unified_name": abr.get("entity_name_raw") or abr.get("entity_name_norm"),
                "unified_name_norm": abr.get("entity_name_norm"),
                "website_domain": cc.get("domain"),
                "website_url_sample": cc.get("url"),
                "industry": cc.get("industry"),
                "entity_type": abr.get("entity_type"),
                "entity_status": abr.get("entity_status"),
                "address_full": abr.get("address_full"),
                "suburb": abr.get("suburb"),
                "postcode": abr.get("postcode"),
                "state": abr.get("state"),
                "start_date": start_date,
                "match_confidence": float(score),
                "match_method": method,
            }

            cur.execute(insert_unified_sql, unified_record)
            company_id = cur.fetchone()[0]

            cur.execute(insert_link_sql, (company_id, "ABR", abr["abn"]))
            cur.execute(
                insert_link_sql, (company_id, "COMMONCRAWL", str(cc["commoncrawl_id"]))
            )

            inserted_count += 1

        conn.commit()
        print(f"✅ Inserted {inserted_count} unified companies and {inserted_count * 2} source links.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error while writing matches to DB: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def main():
    abr_df, cc_df = fetch_staging_data()
    high_conf_matches, ambiguous_matches, unmatched_cc = fuzzy_match_entities(
        abr_df, cc_df, high_threshold=95, low_threshold=0
    )

    llm_approved_matches, remaining_ambiguous = llm_review_ambiguous(
        ambiguous_matches, max_to_review=LLM_MAX_REVIEWS
    )

    all_matches_to_write = high_conf_matches + llm_approved_matches
    write_matches_to_db(all_matches_to_write)

    print("📈 Summary:")
    print(f"  ABR entities (sampled):        {len(abr_df)}")
    print(f"  CC companies (sampled):        {len(cc_df)}")
    print(f"  High-confidence matches:       {len(high_conf_matches)}")
    print(f"  LLM-approved matches:          {len(llm_approved_matches)}")
    print(f"  Total matches written:         {len(all_matches_to_write)}")
    print(f"  Ambiguous remaining:           {len(remaining_ambiguous)}")
    print(f"  Unmatched CC records:          {len(unmatched_cc)}")


if __name__ == "__main__":
    main()
