{{ config(
    materialized = 'table'
) }}

with base as (
    select
        event_date,
        user_pseudo_id,
        first_click_source,
        last_click_source
    from {{ ref('fct_ga4_dashboard') }}
    where event_date >= (
        select date_sub(max(event_date), interval 14 day)
        from {{ ref('fct_ga4_dashboard') }}
    )
)

select
    event_date,
    first_click_source,
    last_click_source,
    count(distinct user_pseudo_id) as user_count
from base
group by event_date, first_click_source, last_click_source
order by event_date desc
