with src as (

    select
        id                   as commoncrawl_id,
        crawl_id,
        url,
        domain,
        tld,
        html_title,
        company_name_raw,
        company_name_norm,
        industry,
        fetched_at
    from raw_commoncrawl

)

select
    commoncrawl_id,
    crawl_id,
    url,
    domain,
    tld,
    html_title,
    company_name_raw,
    company_name_norm,
    industry,
    fetched_at,
    case
        when domain ilike '%.au'
          or domain ilike '%.com.au'
          or domain ilike '%.org.au'
          or domain ilike '%.net.au'
          or url    ilike '%://%.au/%'
        then true
        else false
    end as is_au_domain
from src
