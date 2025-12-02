{{ config(materialized='table')  }}

select

    user_pseudo_id,
    session_id,

    -- Session boundaries
    session_start,
    session_end,

    -- Engagement metrics
    pageviews,
    total_engagement_time_msec,

    -- Campaign attribution
    campaign_source,
    campaign_medium,
    campaign_name,

    -- Device & geo context
    device_category,
    device_os,
    browser,
    country,
    city
from {{ ref("int_sessions") }}
