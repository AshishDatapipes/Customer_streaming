with events as (
    select
        event_name,
        cast(event_timestamp as timestamp) as event_timestamp,  -- convert string → timestamp
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
    from {{ ref('stg_ga4_events') }}
),

sessions as (
    select
        user_id,
        session_id,
        campaign_id,
        source,
        medium,
        env,
        region,
        version,
        publisher,

        -- session timing
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,
        timestamp_diff(max(event_timestamp), min(event_timestamp), second) as session_duration_sec,

        -- engagement metrics
        count(*) as total_events,
        array_agg(distinct event_name) as event_types,
        max(cast(conversion as bool)) as converted,
        array_agg(distinct page_url) as pages_viewed
    from events
    group by
        user_id,
        session_id,
        campaign_id,
        source,
        medium,
        env,
        region,
        version,
        publisher
)

select * from sessions
