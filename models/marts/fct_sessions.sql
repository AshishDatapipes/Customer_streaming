with sessions as (
    select *
    from {{ ref('int_sessions') }}
),

first_click as (
    select
        user_id,
        session_id,
        campaign_id as first_click_campaign,
        source      as first_click_source,
        medium      as first_click_medium
    from (
        select
            user_id,
            session_id,
            campaign_id,
            source,
            medium,
            cast(event_timestamp as timestamp) as event_timestamp,
            row_number() over (
                partition by user_id, session_id
                order by event_timestamp asc
            ) as rn
        from {{ ref('stg_ga4_events') }}
    )
    where rn = 1
),

last_click as (
    select
        user_id,
        session_id,
        campaign_id as last_click_campaign,
        source      as last_click_source,
        medium      as last_click_medium
    from (
        select
            user_id,
            session_id,
            campaign_id,
            source,
            medium,
            cast(event_timestamp as timestamp) as event_timestamp,
            row_number() over (
                partition by user_id, session_id
                order by event_timestamp desc
            ) as rn
        from {{ ref('stg_ga4_events') }}
    )
    where rn = 1
)

select
    s.user_id,
    s.session_id,
    s.campaign_id,
    s.source,
    s.medium,
    s.env,
    s.region,
    s.version,
    s.publisher,

    -- session metrics
    s.session_start,
    s.session_end,
    s.session_duration_sec,
    s.total_events,
    s.event_types,
    s.pages_viewed,
    s.converted,

    -- attribution
    fc.first_click_campaign,
    fc.first_click_source,
    fc.first_click_medium,
    lc.last_click_campaign,
    lc.last_click_source,
    lc.last_click_medium
from sessions s
left join first_click fc
    on s.user_id = fc.user_id and s.session_id = fc.session_id
left join last_click lc
    on s.user_id = lc.user_id and s.session_id = lc.session_id
