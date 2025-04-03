{{
    config(
        materialized='view',
    )
}}

{% set default_event_params = [
    "term",
    "medium",
    "source",
    "percent_scrolled",
    "engaged_session_event",
    "engagement_time_msec",
    "entrances",
    "campaign",
    "ga_session_id",
    "ga_session_number",
    "page_title",
    "session_engaged"
]%}

WITH CTE AS (
  SELECT
    e.event_id,
    e.date AS event_date,
    e.timestamp AS event_timestamp,
    ep.event_name,
    COALESCE(CAST(ep.int_value AS STRING), ep.string_value) as param_value
  FROM {{ ref("stg__google_analytics__event") }} e
  JOIN {{ ref("stg__google_analytics__event_params_original") }} ep
  ON e.event_id = ep.event_id
)

SELECT
    event_id,
    event_date,
    event_timestamp,
    event_name,
    -- Default event params
    MAX(CASE WHEN event_name = 'term' THEN param_value END) as term,
    MAX(CASE WHEN event_name = 'medium' THEN param_value END) as medium,
    MAX(CASE WHEN event_name = 'source' THEN param_value END) as source,
    MAX(CASE WHEN event_name = 'percent_scrolled' THEN param_value END) as percent_scrolled,
    MAX(CASE WHEN event_name = 'engaged_session_event' THEN param_value END) as engaged_session_event,
    MAX(CASE WHEN event_name = 'engagement_time_msec' THEN param_value END) as engagement_time_msec,
    MAX(CASE WHEN event_name = 'entrances' THEN param_value END) as entrances,
    MAX(CASE WHEN event_name = 'campaign' THEN param_value END) as campaign,
    MAX(CASE WHEN event_name = 'ga_session_id' THEN param_value END) as ga_session_id,
    MAX(CASE WHEN event_name = 'ga_session_number' THEN param_value END) as ga_session_number,
    MAX(CASE WHEN event_name = 'page_title' THEN param_value END) as page_title,
    MAX(CASE WHEN event_name = 'session_engaged' THEN param_value END) as session_engaged,
    
    -- Additional params from the original model
    MAX(CASE WHEN event_name = 'content' THEN param_value END) as content,
    MAX(CASE WHEN event_name = 'page_location' THEN param_value END) as page_location,
    MAX(CASE WHEN event_name = 'page_referrer' THEN param_value END) as page_referrer,
    MAX(CASE WHEN event_name = 'value' THEN param_value END) as value
FROM CTE
GROUP BY event_id, event_date, event_timestamp, event_name
