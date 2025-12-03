# CustomerLab Attribution Project

## Overview
This project implements a modular data pipeline for attribution and session analysis using **GA4 → BigQuery → dbt → Looker Studio**.  
The pipeline is designed to be resilient, reviewable, and optimized for performance. It produces attribution-ready fact tables and dashboards for session-level and click-level insights.

---

## Architecture

### Source Layer
- **Data Source:** Google Analytics 4 (GA4) export into BigQuery.
- **Raw Table:** `fct_ga4_dashboard`
- Provides daily event-level data including `event_date`, `user_pseudo_id`, and attribution fields.

### Transformation Layer (dbt Models)

#### Staging Models
- **`stg_ga4_events.sql`**
  - First staging model in the pipeline.
  - Cleans raw GA4 export data from BigQuery.
  - Standardizes field names, types, and prepares event-level data for downstream models.

- **`int_session.sql`**
  - Builds on `stg_ga4_events` to derive session-level information.
  - Ensures schema alignment and prepares fields for fact tables.
  - Handles parsing, deduplication, and type casting.

#### Fact Models
- **`fct_session.sql`**
  - Aggregates session-level metrics (e.g., session counts, user identifiers).
  - Provides the canonical fact table for session analysis.

- **`fct_session_dimension.sql`**
  - Adds descriptive attributes to sessions (e.g., source, medium, campaign).
  - Enables slicing and dicing of session data across multiple dimensions.

- **`fct_last_14days.sql`**
  - Rolling 14-day attribution fact table.
  - Aggregates distinct users by `first_click_source` and `last_click_source`.
  - Powers Looker Studio dashboards for attribution analysis.

---

## Example Model: fct_last_14days

```sql
{{ config(materialized = 'table') }}

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

graph TD
    A[stg_ga4_events] --> B[int_session]
    B --> C[fct_session]
    B --> D[fct_session_dimension]
    C --> E[fct_last_14days]
    D --> E[fct_last_14days]
    E --> F[Looker Studio Dashboard]

Presentation Layer (Looker Studio)
Data Source: BigQuery dataset customer_lab.

Visualizations:

Pie chart: First-click attribution by source.

Pie chart: Last-click attribution by source.

Comparison table: First vs last click counts side by side.

Trend line: Daily attribution counts over 14 days.

Data Refresh Strategy
Materialization: Tables for fast queries.

dbt Cloud Job:

Command: dbt build

Runs all models + tests together.

Scheduled daily to keep attribution and session tables fresh.

Looker Studio:

Data freshness set to 12–24 hours.

Automatically reflects latest dbt job output.

Deliverables
Clean staging model (stg_ga4_events) for GA4 event data.

Intermediate session model (int_session) for session-level preparation.

Fact tables (fct_session, fct_session_dimension) for session-level analysis.

Attribution-ready fact table (fct_last_14days) for rolling attribution insights.

Looker Studio dashboard showing first-click vs last-click attribution and session metrics.

Automated refresh via dbt Cloud job.

Schema validation and tests included in dbt build.

Assessment Context
This project demonstrates:

Ability to design and implement a modular dbt pipeline.

Schema alignment and validation for GA4 → BigQuery exports.

Dashboarding with attribution and session insights.

Automation of refresh cycles for production readiness.

Next Steps
Add medium and campaign dimensions (first_click_medium, last_click_name) for deeper attribution analysis.

Extend dashboard with comparison tables and trend lines.

Explore incremental materialization for scaling with larger datasets.

Add automated tests for attribution and session logic to ensure long-term reliability.

Session-level dashboards using fct_session and fct_session_dimension.
