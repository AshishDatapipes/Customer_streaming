with
    source as (
        select cast(data as string) as data_str from {{ source("ga4", "events_raw") }}
    )

select
    -- message block
    json_value(data_str, '$.message.event_name') as event_name,
    json_value(data_str, '$.message.timestamp') as event_timestamp,
    json_value(data_str, '$.message.campaign_id') as campaign_id,
    json_value(data_str, '$.message.user_id') as user_id,
    json_value(data_str, '$.message.source') as source,
    json_value(data_str, '$.message.medium') as medium,
    json_value(data_str, '$.message.session_id') as session_id,
    json_value(data_str, '$.message.page_url') as page_url,
    cast(json_value(data_str, '$.message.conversion') as bool) as conversion,

    -- attributes block
    json_value(data_str, '$.attributes.env') as env,
    json_value(data_str, '$.attributes.region') as region,
    json_value(data_str, '$.attributes.version') as version,
    json_value(data_str, '$.attributes.publisher') as publisher,

    -- top-level field
    json_value(data_str, '$.ordering_key') as ordering_key
from source
