{{ config(materialized="table", schema="customer_lab") }}

select
    user_pseudo_id,
    event_date,
    min(event_timestamp) as session_start_ts,
    countif(event_name = "page_view") as pageviews,
    sum(engagement_time_msec) as total_engagement_time
from {{ ref("stg_ga4_events") }}
where event_name in ("session_start", "page_view")
group by user_pseudo_id, event_date

