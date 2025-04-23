WITH facebook_ad_sets AS (
    SELECT
        fas.date_day,
        campaign_id,
        fas.campaign_name,
        fas.ad_set_id,
        fas.ad_set_name,
        fas.impressions,
        fas.clicks,
        fas.cost,
        fas.frequency,
        fas.reach,
        fas.calculated_ctr AS ctr,
        fas.calculated_cpc AS cpc,
        fas.calculated_cpm AS cpm,
        fasa.purchases,
        fasa.landing_page_views,
        fasa.leads,
        fas.website_destination,
        fas.campaign_type,
        fas.platform
    FROM {{ ref('int__facebook_ads_ad_sets_daily') }} fas
    LEFT JOIN {{ ref('int__facebook_ads_ad_set_actions') }} fasa 
        ON fas.date_day = fasa.date_day AND fas.ad_set_id = fasa.ad_set_id
),

google_ad_groups AS (
    SELECT
        a.date_day,
        a.campaign_id,
        g.campaign_name,
        a.ad_group_id AS ad_set_id,
        a.ad_group_name AS ad_set_name,
        a.impressions,
        a.clicks,
        a.cost,
        NULL AS frequency,
        NULL AS reach,
        a.ctr,
        a.cpc,
        a.cpm,
        a.conversions AS purchases,
        NULL AS landing_page_views,
        NULL AS leads,
        -- Get website destination and campaign type from campaigns
        g.website_destination,
        g.campaign_type,
        a.platform
    FROM {{ ref('int__google_ads_ad_groups_daily') }} a
    JOIN {{ ref('int__google_ads_campaign_daily') }} g 
        ON a.date_day = g.date_day AND a.campaign_id = g.campaign_id
)

-- Combine data from both platforms
SELECT 
    date_day,
    campaign_id,
    campaign_name,
    ad_set_id,
    ad_set_name,
    impressions,
    clicks,
    cost,
    frequency,
    reach,
    ctr,
    cpc,
    cpm,
    purchases,
    landing_page_views,
    leads,
    website_destination,
    campaign_type,
    platform
FROM facebook_ad_sets

UNION ALL

SELECT 
    date_day,
    campaign_id,
    campaign_name, -- Since we don't have it joined in the Google model
    ad_set_id,
    ad_set_name,
    impressions,
    clicks,
    cost,
    frequency,
    reach,
    ctr,
    cpc,
    cpm,
    purchases,
    landing_page_views, 
    leads,
    website_destination,
    campaign_type,
    platform
FROM google_ad_groups