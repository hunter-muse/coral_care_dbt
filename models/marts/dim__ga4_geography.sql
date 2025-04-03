SELECT
    CONCAT(event_id, '-', geo_city) as geo_sk,
    event_id,
    geo_city as city,
    geo_country as country,
    geo_continent as continent,
    geo_region as region,
    geo_sub_continent as geo_sub_continent,
    geo_metro as metro

FROM {{ ref("stg__google_analytics__event") }}