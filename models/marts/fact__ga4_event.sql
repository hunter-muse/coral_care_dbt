SELECT
    e.event_id,
    e.timestamp as event_timestamp,
    e.BUNDLE_SEQUENCE_ID as event_bundle_sequence_id,
    e.user_first_touch_timestamp,
    e.user_ltv_revenue,
    e.user_pseudo_id,
    ep.medium,
    ep.source,
    ep.campaign,
    ep.term,
    ep.percent_scrolled,
    ep.engaged_session_event
FROM
    {{ ref("stg__google_analytics__event") }} e
JOIN
    {{ ref("stg__google_analytics__event_params_unpacked") }} ep
ON e.event_id = ep.event_id