with parsed as (select * from {{ ref("stg_ga4_events_parsed") }})

select
    event_name,
    event_timestamp,
    campaign_id,
    user_id,
    source,
    medium,
    session_id,
    page_url,
    conversion,
    env,
    region,
    version,
    publisher,
    ordering_key
from parsed
