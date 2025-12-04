-- RAW TABLES

CREATE TABLE IF NOT EXISTS raw_commoncrawl (
    id              BIGSERIAL PRIMARY KEY,
    crawl_id        TEXT NOT NULL,        -- e.g. CC-MAIN-2025-09
    url             TEXT NOT NULL,
    domain          TEXT NOT NULL,
    tld             TEXT,                 -- .com.au, .org.au, etc.
    html_title      TEXT,
    raw_html        TEXT,                 -- optional / can be NULL
    extracted_name  TEXT,                 -- heuristic company name
    extracted_industry TEXT,
    fetched_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_cc_domain ON raw_commoncrawl(domain);
CREATE INDEX IF NOT EXISTS idx_raw_cc_tld ON raw_commoncrawl(tld);


CREATE TABLE IF NOT EXISTS raw_abr (
    id               BIGSERIAL PRIMARY KEY,
    abn              VARCHAR(20) NOT NULL,
    entity_name      TEXT NOT NULL,
    entity_type      TEXT,
    entity_status    TEXT,
    address_line_1   TEXT,
    address_line_2   TEXT,
    suburb           TEXT,
    postcode         VARCHAR(10),
    state            VARCHAR(10),
    country          TEXT,
    start_date_raw   TEXT,         -- as in XML; we'll normalise later
    load_batch_id    TEXT,         -- filename or batch identifier
    loaded_at        TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_abr_abn ON raw_abr(abn);
CREATE INDEX IF NOT EXISTS idx_raw_abr_state_postcode ON raw_abr(state, postcode);


-- UNIFIED COMPANY TABLES

CREATE TABLE IF NOT EXISTS company_unified (
    company_id          BIGSERIAL PRIMARY KEY,
    abn                 VARCHAR(20),      -- may be NULL for non-matched web-only
    unified_name        TEXT NOT NULL,
    unified_name_norm   TEXT NOT NULL,
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
    match_confidence    NUMERIC(5,2),     -- 0-100
    match_method        TEXT,             -- 'exact', 'fuzzy_name', 'llm_approved', etc.
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_company_unified_abn ON company_unified(abn);
CREATE INDEX IF NOT EXISTS idx_company_unified_domain ON company_unified(website_domain);
CREATE INDEX IF NOT EXISTS idx_company_unified_state_postcode ON company_unified(state, postcode);


CREATE TABLE IF NOT EXISTS company_source_link (
    company_id      BIGINT REFERENCES company_unified(company_id) ON DELETE CASCADE,
    source_system   TEXT NOT NULL,        -- 'ABR' or 'COMMONCRAWL'
    source_key      TEXT NOT NULL,        -- e.g. abn or raw_commoncrawl.id as string
    PRIMARY KEY (company_id, source_system, source_key)
);
