WITH event_params AS (
  SELECT
    _fivetran_id as event_id,
    param_campaign,
    param_content,
    param_engagement_time_msec,
    param_ga_session_id,
    param_ga_session_number,
    param_medium,
    param_page_location,
    param_page_referrer,
    param_page_title,
    param_percent_scrolled,
    param_session_engaged,
    param_source,
    param_term,
    param_value
  FROM {{ source("google_analytics_raw", "event") }}
),

campaign_params AS (
  SELECT
    event_id,
    'campaign' AS event_name,
    NULL AS int_value,
    param_campaign AS string_value
  FROM event_params
  WHERE param_campaign IS NOT NULL
),

content_params AS (
  SELECT
    event_id,
    'content' AS event_name,
    NULL AS int_value,
    param_content AS string_value
  FROM event_params
  WHERE param_content IS NOT NULL
),

engagement_time_params AS (
  SELECT
    event_id,
    'engagement_time_msec' AS event_name,
    CASE
      WHEN param_engagement_time_msec IS NOT NULL AND 
           CAST(param_engagement_time_msec AS VARCHAR) REGEXP '^[0-9]+$'
      THEN CAST(param_engagement_time_msec AS INT)
      ELSE NULL
    END AS int_value,
    CASE
      WHEN param_engagement_time_msec IS NOT NULL AND 
           NOT (CAST(param_engagement_time_msec AS VARCHAR) REGEXP '^[0-9]+$')
      THEN CAST(param_engagement_time_msec AS VARCHAR)
      ELSE NULL
    END AS string_value
  FROM event_params
  WHERE param_engagement_time_msec IS NOT NULL
),

ga_session_id_params AS (
  SELECT
    event_id,
    'ga_session_id' AS event_name,
    CASE
      WHEN param_ga_session_id IS NOT NULL AND 
           CAST(param_ga_session_id AS VARCHAR) REGEXP '^[0-9]+$'
      THEN CAST(param_ga_session_id AS INT)
      ELSE NULL
    END AS int_value,
    CASE
      WHEN param_ga_session_id IS NOT NULL AND 
           NOT (CAST(param_ga_session_id AS VARCHAR) REGEXP '^[0-9]+$')
      THEN CAST(param_ga_session_id AS VARCHAR)
      ELSE NULL
    END AS string_value
  FROM event_params
  WHERE param_ga_session_id IS NOT NULL
),

ga_session_number_params AS (
  SELECT
    event_id,
    'ga_session_number' AS event_name,
    CASE
      WHEN param_ga_session_number IS NOT NULL AND 
           CAST(param_ga_session_number AS VARCHAR) REGEXP '^[0-9]+$'
      THEN CAST(param_ga_session_number AS INT)
      ELSE NULL
    END AS int_value,
    CASE
      WHEN param_ga_session_number IS NOT NULL AND 
           NOT (CAST(param_ga_session_number AS VARCHAR) REGEXP '^[0-9]+$')
      THEN CAST(param_ga_session_number AS VARCHAR)
      ELSE NULL
    END AS string_value
  FROM event_params
  WHERE param_ga_session_number IS NOT NULL
),

medium_params AS (
  SELECT
    event_id,
    'medium' AS event_name,
    NULL AS int_value,
    param_medium AS string_value
  FROM event_params
  WHERE param_medium IS NOT NULL
),

page_location_params AS (
  SELECT
    event_id,
    'page_location' AS event_name,
    NULL AS int_value,
    param_page_location AS string_value
  FROM event_params
  WHERE param_page_location IS NOT NULL
),

page_referrer_params AS (
  SELECT
    event_id,
    'page_referrer' AS event_name,
    NULL AS int_value,
    param_page_referrer AS string_value
  FROM event_params
  WHERE param_page_referrer IS NOT NULL
),

page_title_params AS (
  SELECT
    event_id,
    'page_title' AS event_name,
    NULL AS int_value,
    param_page_title AS string_value
  FROM event_params
  WHERE param_page_title IS NOT NULL
),

percent_scrolled_params AS (
  SELECT
    event_id,
    'percent_scrolled' AS event_name,
    CASE
      WHEN param_percent_scrolled IS NOT NULL AND 
           CAST(param_percent_scrolled AS VARCHAR) REGEXP '^[0-9]+$'
      THEN CAST(param_percent_scrolled AS INT)
      ELSE NULL
    END AS int_value,
    CASE
      WHEN param_percent_scrolled IS NOT NULL AND 
           NOT (CAST(param_percent_scrolled AS VARCHAR) REGEXP '^[0-9]+$')
      THEN CAST(param_percent_scrolled AS VARCHAR)
      ELSE NULL
    END AS string_value
  FROM event_params
  WHERE param_percent_scrolled IS NOT NULL
),

session_engaged_params AS (
  SELECT
    event_id,
    'session_engaged' AS event_name,
    CASE
      WHEN param_session_engaged IS NOT NULL AND 
           CAST(param_session_engaged AS VARCHAR) REGEXP '^[0-9]+$'
      THEN CAST(param_session_engaged AS INT)
      ELSE NULL
    END AS int_value,
    CASE
      WHEN param_session_engaged IS NOT NULL AND 
           NOT (CAST(param_session_engaged AS VARCHAR) REGEXP '^[0-9]+$')
      THEN CAST(param_session_engaged AS VARCHAR)
      ELSE NULL
    END AS string_value
  FROM event_params
  WHERE param_session_engaged IS NOT NULL
),

source_params AS (
  SELECT
    event_id,
    'source' AS event_name,
    NULL AS int_value,
    param_source AS string_value
  FROM event_params
  WHERE param_source IS NOT NULL
),

term_params AS (
  SELECT
    event_id,
    'term' AS event_name,
    NULL AS int_value,
    param_term AS string_value
  FROM event_params
  WHERE param_term IS NOT NULL
),

value_params AS (
  SELECT
    event_id,
    'value' AS event_name,
    CASE
      WHEN param_value IS NOT NULL AND 
           CAST(param_value AS VARCHAR) REGEXP '^[0-9]+(\.[0-9]+)?$'
      THEN CAST(param_value AS FLOAT)
      ELSE NULL
    END AS int_value,
    CASE
      WHEN param_value IS NOT NULL AND 
           NOT (CAST(param_value AS VARCHAR) REGEXP '^[0-9]+(\.[0-9]+)?$')
      THEN CAST(param_value AS VARCHAR)
      ELSE NULL
    END AS string_value
  FROM event_params
  WHERE param_value IS NOT NULL
)

SELECT * FROM campaign_params
UNION ALL SELECT * FROM content_params
UNION ALL SELECT * FROM engagement_time_params
UNION ALL SELECT * FROM ga_session_id_params
UNION ALL SELECT * FROM ga_session_number_params
UNION ALL SELECT * FROM medium_params
UNION ALL SELECT * FROM page_location_params
UNION ALL SELECT * FROM page_referrer_params
UNION ALL SELECT * FROM page_title_params
UNION ALL SELECT * FROM percent_scrolled_params
UNION ALL SELECT * FROM session_engaged_params
UNION ALL SELECT * FROM source_params
UNION ALL SELECT * FROM term_params
UNION ALL SELECT * FROM value_params
WHERE event_name IS NOT NULL