WITH facebook_campaigns AS (
    SELECT DISTINCT
        campaign_id,
        campaign_name,
        website_destination,
        campaign_type,
        'Facebook' AS platform
    FROM {{ ref('int__facebook_ads_campaign_daily') }}
),

google_campaigns AS (
    SELECT DISTINCT
        campaign_id,
        campaign_name,
        website_destination, 
        campaign_type,
        'Google' AS platform
    FROM {{ ref('int__google_ads_campaign_daily') }}
)

SELECT 
    campaign_id,
    campaign_name,
    website_destination,
    campaign_type,
    platform
FROM facebook_campaigns

UNION ALL

SELECT 
    campaign_id,
    campaign_name,
    website_destination,
    campaign_type,
    platform
FROM google_campaigns