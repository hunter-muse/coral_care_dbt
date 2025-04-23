SELECT
    event_id,
   --page_path,
    PARAM_PAGE_TITLE as page_title,
    PARAM_PAGE_REFERRER AS page_referrer,
    PARAM_PAGE_LOCATION AS page_location,
    PARAM_BATCH_PAGE_ID AS batch_page_id,
    BATCH_PAGE_ID AS original_batch_page_id
FROM
    {{ ref("stg__google_analytics__event") }}
