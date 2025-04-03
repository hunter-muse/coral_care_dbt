SELECT
    event_id,
    ga_session_id,
    session_engaged,
    ga_session_number,
    engagement_time_msec,
    entrances
FROM
    {{ ref("stg__google_analytics__event_params_unpacked") }}