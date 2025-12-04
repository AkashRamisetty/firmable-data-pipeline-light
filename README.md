# firmable-data-pipeline
End-to-end data pipeline for ABR + Common Crawl: Postgres, dbt, Python ETL, and LLM-assisted entity matching.

## 1. Problem Overview

This project implements an end-to-end data pipeline that:

- Extracts Australian business entities from the **Australian Business Register (ABR)** bulk XML.
- Extracts website records from the **Common Crawl March 2025 index** (sampled to 100k records for local development).
- Loads both sources into a **PostgreSQL** warehouse running in Docker.
- Uses **dbt** for staging / cleaning / normalisation.
- Performs **entity matching** between ABR entities and Common Crawl websites.
- Demonstrates how **LLMs can improve entity matching quality** on a smaller, cost-controlled subset of data.

The goal is to produce a unified view of companies in a `company_unified` table, with lineage in `company_source_link`.

---

## 2. High-Level Architecture

```mermaid
flowchart LR
    A[ABR Bulk XML (zips)] --> B[Postgres: raw_abr]
    C[Common Crawl CC-MAIN-2025-13 index (gz)] --> D[Postgres: raw_commoncrawl]

    B --> E[dbt: stg_abr_entities]
    D --> F[dbt: stg_commoncrawl_companies]

    E --> G[company_unified]
    F --> G

    G --> H[company_source_link]
```



Tech stack

PostgreSQL in Docker (via docker-compose)

Python (3.11, psycopg2, pandas, rapidfuzz, lxml, tqdm, openai)

dbt-core + dbt-postgres

OpenAI GPT-4.1-mini for LLM-assisted matching (optional, small subset)

## 3. Data Sources
3.1 Australian Business Register (ABR)

Source: Bulk XML files (Public ABR extract).

Ingest pipeline:

src/extract_abr_bulk.py

Streams XML directly from zipped files (no full unzip to disk).

Normalises both legal entity names and trading names into a single entity_name.

Extracts:

abn

entity_name

entity_type

entity_status

state

postcode

start_date_raw

Loaded into: raw_abr (Postgres).

Stats from my run

raw_abr rows: 19,735,506

stg_abr_entities rows: 19,735,506
(all raw records are carried through into the staging model with cleaned / normalised fields)

3.2 Common Crawl (March 2025 Index)

Source: CC-MAIN-2025-13 index CDX file (e.g. cc-index-cdx-00000.gz).

Ingest pipeline:

src/extract_commoncrawl_ccindex.py

Streams the gzipped CDX file line-by-line and parses the trailing JSON.

For this assignment, I load the first 100,000 index entries to keep runtime and storage manageable on a laptop.

Extracted fields:

crawl_id

url

domain

tld

company_name_raw (simple heuristic from hostname; not authoritative)

company_name_norm (upper-cased / cleaned)

industry (placeholder; real extraction would use HTML parsing / NER, see “Future work”)

Loaded into: raw_commoncrawl.

Stats from my run

raw_commoncrawl rows: 100,000

stg_commoncrawl_companies rows: 100,000

## 4. Database Schema (DDL)

All DDL is located in:

sql/init_db.sql

Key tables:

4.1 Raw layer
CREATE TABLE raw_abr (
    id              BIGSERIAL PRIMARY KEY,
    abn             VARCHAR(20) NOT NULL,
    entity_name     TEXT        NOT NULL,
    entity_type     TEXT,
    entity_status   TEXT,
    address_full    TEXT,
    suburb          TEXT,
    state           VARCHAR(10),
    postcode        VARCHAR(10),
    start_date_raw  VARCHAR(20),
    country_code    VARCHAR(3) DEFAULT 'AU',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE raw_commoncrawl (
    id               BIGSERIAL PRIMARY KEY,
    commoncrawl_id   BIGINT      NOT NULL,
    crawl_id         TEXT        NOT NULL,
    url              TEXT        NOT NULL,
    domain           TEXT        NOT NULL,
    tld              TEXT,
    html_title       TEXT,
    company_name_raw TEXT,
    company_name_norm TEXT,
    industry         TEXT,
    fetched_at       TIMESTAMPTZ DEFAULT NOW()
);

4.2 Unified company model
CREATE TABLE company_unified (
    company_id          BIGSERIAL PRIMARY KEY,
    abn                 VARCHAR(20),
    unified_name        TEXT,
    unified_name_norm   TEXT,
    website_domain      TEXT,
    website_url_sample  TEXT,
    industry            TEXT,
    entity_type         TEXT,
    entity_status       TEXT,
    address_full        TEXT,
    suburb              TEXT,
    postcode            VARCHAR(10),
    state               VARCHAR(10),
    start_date          DATE,
    match_confidence    NUMERIC(5,2),
    match_method        TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE company_source_link (
    id            BIGSERIAL PRIMARY KEY,
    company_id    BIGINT      NOT NULL REFERENCES company_unified(company_id),
    source_system TEXT        NOT NULL,
    source_key    TEXT        NOT NULL
);

Indexes
CREATE INDEX idx_raw_abr_abn ON raw_abr(abn);
CREATE INDEX idx_raw_commoncrawl_domain ON raw_commoncrawl(domain);
CREATE INDEX idx_company_unified_abn ON company_unified(abn);
CREATE INDEX idx_company_unified_domain ON company_unified(website_domain);

## 5. dbt Models & Data Quality
5.1 Project layout

dbt_project/firmable_dbt/dbt_project.yml

Sources:

raw_sources.yml:

raw_abr

raw_commoncrawl

Models:

models/staging/stg_abr_entities.sql

models/staging/stg_commoncrawl_companies.sql

5.2 Example: stg_abr_entities

Key transformations:

Upper-case and strip punctuation from entity names → entity_name_norm.

Combine legal name and trading name in a consistent way.

Keep only Australian entities with a valid abn and state.

Keep start_date_raw in a canonical YYYYMMDD or YYYY-MM-DD format for downstream parsing.

5.3 Data tests

Not null: abn, entity_name_norm in stg_abr_entities.

Uniqueness: (abn) in stg_abr_entities (logical uniqueness).

Basic accepted values: entity_status IN ('ACT','CAN',...).

Run commands:
cd dbt_project/firmable_dbt
dbt run --select stg_abr_entities stg_commoncrawl_companies
dbt test

## 6. Entity Matching Strategy

Entity matching is implemented in:

src/entity_matching.py

6.1 Sampling for local development

Full volumes:

stg_abr_entities: ~19.7M entities

stg_commoncrawl_companies: 100k records

For a local M4 MacBook and quick iteration, the matching script operates on sampled data:
-- ABR sample in fetch_staging_data()
WHERE state IS NOT NULL
  AND entity_status = 'ACT'
  AND (abn::bigint % 5000) = 0;

-- Common Crawl sample
WHERE (commoncrawl_id % 20) = 0;

This yields approximately:

422 ABR entities

5,000 Common Crawl companies

6.2 Fuzzy matching

For each Common Crawl company:

Take company_name_norm.

Compare to every entity_name_norm in the sampled ABR DataFrame using rapidfuzz.fuzz.token_sort_ratio.

Keep the best-scoring ABR candidate and its score.

Thresholds:

high_threshold = 95 → intended for automatic matches.

low_threshold = 0 → in this demo, we treat all non-zero scores as ambiguous to showcase the LLM stage.

In the real sample I used, the Common Crawl slice is dominated by IP-style hostnames (e.g. "167", "3", etc.), so name-only fuzzy matching does not find robust high-confidence pairs. In practice, I’d enrich the CC data with better company signals (page titles, schema.org org names, etc.).

## 7. LLM-Assisted Matching (Key Requirement)
7.1 Motivation

The assignment calls for:

“With a smaller volume of data (given cost considerations), demonstrate how you would use LLMs to make this process more accurate and efficient.”

Instead of sending millions of pairs to an LLM, this pipeline:

Uses SQL + fuzzy string matching to narrow down candidates.

Sends only a small sampled subset of ambiguous matches to an LLM for a second opinion.

7.2 Implementation

File: src/entity_matching.py

Key pieces:

Environment variables:

OPENAI_API_KEY (not committed to Git)

OPENAI_MODEL (default: gpt-4.1-mini)

Client:
from openai import OpenAI
openai_client = OpenAI()

For each ambiguous pair, a compact prompt is constructed containing:

Website-side fields:

company_name_norm

url

domain

ABR-side fields:

abn

entity_name_norm / entity_name_raw

entity_type

entity_status

state, postcode, address_full

Example (from an actual logged prompt):
You are matching Australian companies between a website (Common Crawl) and an ABR record.

Common Crawl company:
- Normalised name: 167
- URL: http://167.172.14.0/
- Domain: 167.172.14.0

ABR candidate:
- ABN: 64651645000
- Entity name (normalised): ACN 651645000 PTY LTD
- Entity name (raw): ACN 651645000 PTY LTD
- Entity type: Australian Private Company
- Status: ACT
- Address: QLD, 4214, , QLD 4214

Question:
Are these records referring to the same underlying company?

Respond **only** with a JSON object with the following shape:
{
  "is_match": true or false,
  "confidence": "low" | "medium" | "high",
  "reason": "short explanation here"
}

A typical LLM response (logged in data/llm_match_logs.jsonl) looks like:
{
  "is_match": false,
  "confidence": "high",
  "reason": "The Common Crawl company name '167' and domain '167.172.14.0' do not match the ABR entity name 'ACN 651645000 PTY LTD' or its ABN. There is no clear connection between the IP address and the company name or ABN."
}

We parse the JSON and only accept matches where:

is_match = true, and

confidence ∈ {"high", "medium"}.

Approved matches are stamped with match_method = 'llm_disambiguation' and written to company_unified.

In one of my runs, a response did not come back as clean JSON, resulting in json.loads failing with:

Expecting value: line 1 column 1 (char 0)

In a production setting I would:

Add a JSON-cleaner that strips code fences and extracts the JSON object.

Add retries and schema validation.

7.3 Cost control

LLM usage is hard-limited via:
LLM_MAX_REVIEWS = 10

Even if there are 5,000 ambiguous pairs, at most 10 are sent to the LLM per run.
Using gpt-4.1-mini, this keeps costs well under a few cents per run.

## 8. Data Quality & Deduplication

Raw layer:

ABR: one row per ABN after XML parsing.

Common Crawl: one row per index entry (domains may appear multiple times).

Staging layer (dbt):

Normalises names and addresses.

dbt tests for:

not_null

unique on key fields where reasonable.

simple accepted_values.

Unified layer:

company_unified is populated by entity_matching.py.

company_source_link stores the linkage:

One unified company_id.

Two source systems per match: "ABR" (ABN) and "COMMONCRAWL" (Common Crawl record ID).

## 9. Setup & Running Instructions
9.1 Prerequisites

macOS (tested on Apple Silicon)

Docker Desktop

Python 3.11

pip / venv

dbt-core & dbt-postgres

9.2 Clone and environment
git clone https://github.com/<your-username>/firmable-data-pipeline.git
cd firmable-data-pipeline

python -m venv venv
source venv/bin/activate

pip install -r requirements.txt

9.3 Start Postgres in Docker
docker-compose up -d

9.4 Initialise DB schema
python src/init_db.py

9.5 Load ABR bulk data

Place ABR zips under:

data/abr/ (e.g. public_split_1_10.zip, public_split_11_20.zip)

Then:
python src/extract_abr_bulk.py

9.6 Load Common Crawl index sample

Place the CC index file under:

data/commoncrawl/cc-index-cdx-00000.gz

Then:
python src/extract_commoncrawl_ccindex.py

9.7 Run dbt staging models
cd dbt_project/firmable_dbt
dbt debug
dbt run --select stg_abr_entities stg_commoncrawl_companies
dbt test

9.8 Run entity matching (with optional LLM)

Back in repo root:
cd ~/projects/firmable-data-pipeline
source venv/bin/activate

# optional: enable LLM review
export OPENAI_API_KEY="sk-..."         # not committed to git
export OPENAI_MODEL="gpt-4.1-mini"

python src/entity_matching.py

## 10. Statistics Summary

From my latest run:

raw_abr rows: 19,735,506

stg_abr_entities rows: 19,735,506

raw_commoncrawl rows: 100,000

stg_commoncrawl_companies rows: 100,000

company_unified rows: 2
(these are from a small earlier demo run on synthetic data; the sampled real CC slice currently produces no additional high-confidence or LLM-approved matches due to minimal overlap between IP-style hostnames and ABR company names)

company_source_link rows: 4 (2 source rows per unified company)

## 11. Design Choices & Trade-offs

Postgres + dbt
Simple, transparent, and easy to run locally inside Docker. dbt handles SQL-heavy cleaning and quality checks; Postgres holds raw, staged, and unified layers.

Python ETL
Python is well suited for:

Streaming large gzipped XML / CDX files.

Complex parsing and logging.

Orchestrating matching & LLM calls.

Fuzzy matching first, LLM second
Fuzzy matching (RapidFuzz) is fast and cheap for large volumes. LLMs are used only as a second-stage reviewer on a small subset of ambiguous pairs, which is cost-efficient and interpretable.

Sampling for laptop-friendly runs
The full ABR dataset is ~20M rows. For this assignment on a personal laptop, I use SQL sampling (modulus filters) to keep matching runs fast while maintaining a realistic design that can scale.

## 12. IDE Used

IDE: (e.g.) VS Code on macOS

Environment: macOS on Apple Silicon (M4), Docker Desktop, Python 3.11 virtual environment, dbt-postgres.

## 13. Future Improvements

If productionised, I would:

Enhance Common Crawl company extraction by:

Parsing HTML titles.

Using schema.org / OpenGraph organisation metadata.

Applying an NER model to detect organisation names.

Introduce blocking (e.g. state + first token of name) before fuzzy matching to scale matching to all 19.7M ABR records.

Harden the LLM JSON parsing (strip code fences, validate schema, retry on failure).

Add orchestration (Airflow / Dagster) and CI for dbt and ETL scripts.

