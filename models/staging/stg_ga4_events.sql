{{ config(materialized='table') }}

with
    unnested as (
        select
            event_date,
            event_timestamp,
            event_name,
            user_pseudo_id,
            ep.key,
            ep.value.string_value as string_val,
            ep.value.int_value as int_val,
            device.category as device_category,
            device.operating_system as device_os,
            device.web_info.browser as browser,
            geo.country as country,
            geo.city as city,
            traffic_source.name as campaign_name,
            traffic_source.medium as campaign_medium,
            traffic_source.source as campaign_source
        from {{ source("ga4_public", "events_20210131") }}, unnest(event_params) as ep
    )

select
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,

    max(case when key = "page_location" then string_val end) as page_location,
    max(case when key = "page_referrer" then string_val end) as page_referrer,
    max(
        case when key = "engagement_time_msec" then int_val end
    ) as engagement_time_msec,

    device_category,
    device_os,
    browser,
    country,
    city,
    campaign_name,
    campaign_medium,
    campaign_source
from unnested
group by
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    device_category,
    device_os,
    browser,
    country,
    city,
    campaign_name,
    campaign_medium,
    campaign_source

