{{ config(
    materialized='incremental',
    unique_key='session_id'
) }}

WITH base AS (
    SELECT
        event_date,
        event_timestamp,
        user_pseudo_id,
        event_name,
        page_location,
        page_referrer,
        engagement_time_msec,
        device_category,
        device_os,
        browser,
        country,
        city,
        campaign_name,
        campaign_medium,
        campaign_source
    FROM {{ ref('stg_ga4_events') }}
),

-- Order events per user
ordered AS (
    SELECT
        *,
        TIMESTAMP_MICROS(event_timestamp) AS event_ts,
        LAG(TIMESTAMP_MICROS(event_timestamp)) OVER (
            PARTITION BY user_pseudo_id ORDER BY event_timestamp
        ) AS prev_event_ts
    FROM base
),

-- Flag new sessions when gap > 30 minutes or first event
session_flags AS (
    SELECT
        *,
        CASE
            WHEN prev_event_ts IS NULL THEN 1
            WHEN TIMESTAMP_DIFF(event_ts, prev_event_ts, MINUTE) > 30 THEN 1
            ELSE 0
        END AS new_session_flag
    FROM ordered
),

-- Assign session ids
sessionized AS (
    SELECT
        *,
        SUM(new_session_flag) OVER (
            PARTITION BY user_pseudo_id ORDER BY event_ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_number
    FROM session_flags
),

final AS (
    SELECT
        user_pseudo_id,
        CONCAT(user_pseudo_id, '-', session_number) AS session_id,
        MIN(event_ts) AS session_start,
        MAX(event_ts) AS session_end,
        COUNTIF(event_name = 'page_view') AS pageviews,
        SUM(engagement_time_msec) AS total_engagement_time_msec,
        ARRAY_AGG(DISTINCT device_category IGNORE NULLS)[OFFSET(0)] AS device_category,
        ARRAY_AGG(DISTINCT device_os IGNORE NULLS)[OFFSET(0)] AS device_os,
        ARRAY_AGG(DISTINCT browser IGNORE NULLS)[OFFSET(0)] AS browser,
        ARRAY_AGG(DISTINCT country IGNORE NULLS)[OFFSET(0)] AS country,
        ARRAY_AGG(DISTINCT city IGNORE NULLS)[OFFSET(0)] AS city,
        ARRAY_AGG(DISTINCT campaign_name IGNORE NULLS)[OFFSET(0)] AS campaign_name,
        ARRAY_AGG(DISTINCT campaign_medium IGNORE NULLS)[OFFSET(0)] AS campaign_medium,
        ARRAY_AGG(DISTINCT campaign_source IGNORE NULLS)[OFFSET(0)] AS campaign_source
    FROM sessionized
    GROUP BY user_pseudo_id, session_number
)

SELECT * FROM final
