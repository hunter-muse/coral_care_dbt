SELECT
    CONCAT(event_id, '-', traffic_source_name) as traffic_sk,
    event_id,
    traffic_source_name,
    traffic_source_medium,
    traffic_source_source
    --we can define paid sources in this model if we know what they are
FROM
    {{ ref("stg__google_analytics__event") }}