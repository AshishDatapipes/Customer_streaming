{{ config(materialized='table', schema='customer_lab') }}

SELECT
  -- Core identifiers
  user_pseudo_id,
  event_date,
  MIN(event_timestamp) AS session_start_ts,
  MAX(event_timestamp) AS session_end_ts,

  -- Campaign attribution
  campaign_source,
  campaign_medium,
  campaign_name,

  -- Engagement metrics
  COUNTIF(event_name = "session_start") AS sessions,
  COUNTIF(event_name = "page_view") AS pageviews,
  COUNTIF(event_name = "purchase") AS purchases,
  SUM(engagement_time_msec) AS total_engagement_time,

  -- Device & geo context
  device_category,
  device_os,
  browser,
  country,
  city
FROM {{ ref('stg_ga4_events') }}
WHERE event_name IN ("session_start", "page_view", "purchase")
GROUP BY
  user_pseudo_id,
  event_date,
  campaign_source,
  campaign_medium,
  campaign_name,
  device_category,
  device_os,
  browser,
  country,
  city
