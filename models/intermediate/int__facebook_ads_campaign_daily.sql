-- models/intermediate/facebook_ads/int_facebook_ads_campaigns_daily.sql
SELECT
    date AS date_day,
    campaign_id,
    campaign_name,
    account_id,
    impressions,
    inline_link_clicks AS clicks,
    reach,
    cost_per_inline_link_click,
    cpc,
    cpm,
    ctr,
    frequency,
    spend AS cost,
    inline_link_click_ctr,
    -- Standardized metrics for cross-platform comparison
    inline_link_clicks / NULLIF(impressions, 0) AS calculated_ctr,
    spend / NULLIF(inline_link_clicks, 0) AS calculated_cpc,
    spend / NULLIF(impressions, 0) * 1000 AS calculated_cpm,
    -- Website classification
    CASE 
        WHEN LOWER(campaign_name) LIKE '%joincoralcare%' THEN 'JoinCoralCare'
        WHEN LOWER(campaign_name) LIKE '%provider%' THEN 'Provider Microsite'
        ELSE 'Other'
    END AS website_destination,
    -- Campaign purpose classification  
    CASE
        WHEN LOWER(campaign_name) LIKE '%retargeting%' THEN 'Retargeting'
        WHEN LOWER(campaign_name) LIKE '%prospecting%' THEN 'Prospecting'
        WHEN LOWER(campaign_name) LIKE '%boosted%' THEN 'Boosted Post'
        ELSE 'Other'
    END AS campaign_type,
    'Facebook' AS platform
FROM {{ ref('stg__facebook_ads__basic_campaign') }}