-- models/marts/marketing/mrt_campaign_performance.sql
WITH facebook_campaigns AS (
    SELECT
        date_day,
        campaign_id,
        campaign_name,
        impressions,
        clicks,
        cost,
        frequency,
        reach,
        calculated_ctr AS ctr,
        calculated_cpc AS cpc,
        calculated_cpm AS cpm,
        NULL AS conversions,
        NULL AS conversions_value,
        website_destination,
        campaign_type,
        platform
    FROM {{ ref('int__facebook_ads_campaign_daily') }}
),

google_campaigns AS (
    SELECT
        date_day,
        campaign_id,
        campaign_name,
        impressions,
        clicks,
        cost,
        NULL AS frequency,
        NULL AS reach,
        ctr,
        cpc,
        cpm,
        conversions,
        conversions_value,
        website_destination,
        campaign_type,
        platform
    FROM {{ ref('int__google_ads_campaign_daily') }}
)

SELECT 
    date_day,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    cost,
    frequency,
    reach,
    ctr,
    cpc,
    cpm,
    conversions,
    conversions_value,
    website_destination,
    campaign_type,
    platform
FROM facebook_campaigns

UNION ALL

SELECT 
    date_day,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    cost,
    frequency,
    reach,
    ctr,
    cpc,
    cpm,
    conversions,
    conversions_value,
    website_destination,
    campaign_type,
    platform
FROM google_campaigns