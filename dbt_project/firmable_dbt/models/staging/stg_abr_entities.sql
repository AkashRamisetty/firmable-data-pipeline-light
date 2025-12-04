{{ config(materialized='table') }}

WITH raw AS (
    SELECT
        id,
        abn,
        entity_name,
        entity_type,
        entity_status,
        address_line_1,
        address_line_2,
        suburb,
        postcode,
        state,
        country,
        start_date_raw,
        load_batch_id,
        loaded_at
    FROM public.raw_abr
),

cleaned AS (
    SELECT
        abn,
        UPPER(TRIM(entity_name)) AS entity_name_norm,
        entity_name               AS entity_name_raw,
        entity_type,
        entity_status,
        CONCAT_WS(', ',
            NULLIF(address_line_1, ''),
            NULLIF(address_line_2, ''),
            NULLIF(suburb, ''),
            NULLIF(state, ''),
            NULLIF(postcode, '')
        ) AS address_full,
        suburb,
        postcode,
        UPPER(TRIM(state)) AS state,
        country,
        start_date_raw,
        load_batch_id,
        loaded_at
    FROM raw
)

SELECT * FROM cleaned
