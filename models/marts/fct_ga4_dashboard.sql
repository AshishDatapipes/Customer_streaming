{{ config(materialized='table') }}

SELECT
  DATE(session_start) AS event_date,
  user_pseudo_id,
  session_id,

  -- Engagement metrics
  pageviews,
  total_engagement_time_msec,

  -- First‑click attribution (campaign at session start)
  FIRST_VALUE(campaign_source) OVER (
    PARTITION BY user_pseudo_id, session_id
    ORDER BY session_start
  ) AS first_click_source,
  FIRST_VALUE(campaign_medium) OVER (
    PARTITION BY user_pseudo_id, session_id
    ORDER BY session_start
  ) AS first_click_medium,
  FIRST_VALUE(campaign_name) OVER (
    PARTITION BY user_pseudo_id, session_id
    ORDER BY session_start
  ) AS first_click_name,

  -- Last‑click attribution (campaign at session end)
  LAST_VALUE(campaign_source) OVER (
    PARTITION BY user_pseudo_id, session_id
    ORDER BY session_end
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS last_click_source,
  LAST_VALUE(campaign_medium) OVER (
    PARTITION BY user_pseudo_id, session_id
    ORDER BY session_end
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS last_click_medium,
  LAST_VALUE(campaign_name) OVER (
    PARTITION BY user_pseudo_id, session_id
    ORDER BY session_end
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS last_click_name,

  -- Device & geo context
  device_category,
  device_os,
  browser,
  country,
  city
FROM {{ ref('fct_sessions') }}
