-- models/marts/marketing/fact_conversions.sql
WITH facebook_conversions AS (
    SELECT
        date_day,
        campaign_id,
        campaign_name,
        ad_set_id,
        ad_set_name,
        website_destination,
        campaign_type,
        platform,
        purchases,
        landing_page_views,
        leads,
        cost
    FROM {{ ref('fact__ad_set_performance') }}
    WHERE platform = 'Facebook'
),

google_conversions AS (
    SELECT
        date_day,
        campaign_id,
        campaign_name,
        ad_set_id,
        ad_set_name,
        website_destination,
        campaign_type,
        platform,
        purchases,
        landing_page_views,
        leads,
        cost
    FROM {{ ref('fact__ad_set_performance') }}
    WHERE platform = 'Google'
)

SELECT * FROM facebook_conversions
UNION ALL
SELECT * FROM google_conversions
