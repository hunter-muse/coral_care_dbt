-- models/intermediate/facebook_ads/int_facebook_ads_ad_sets_daily.sql
SELECT
    bas.date AS date_day,
    adset_id AS ad_set_id,
    bas.adset_name AS ad_set_name,
    bc.campaign_id,
    bas.campaign_name,
    bas.account_id,
    bas.impressions,
    bas.inline_link_clicks AS clicks,
    bas.reach,
    bas.cost_per_inline_link_click,
    bas.cpc,
    bas.cpm,
    bas.ctr,
    bas.frequency,
    bas.spend AS cost,
    bas.inline_link_click_ctr,
    -- Standardized metrics for cross-platform comparison
    bas.inline_link_clicks / NULLIF(bas.impressions, 0) AS calculated_ctr,
    bas.spend / NULLIF(bas.inline_link_clicks, 0) AS calculated_cpc,
    bas.spend / NULLIF(bas.impressions, 0) * 1000 AS calculated_cpm,
    -- Website classification
    CASE 
        WHEN LOWER(bas.campaign_name) LIKE '%joincoralcare%' THEN 'JoinCoralCare'
        WHEN LOWER(bas.campaign_name) LIKE '%provider%' THEN 'Provider Microsite'
        ELSE 'Other'
    END AS website_destination,
    -- Campaign purpose classification
    CASE
        WHEN LOWER(bas.campaign_name) LIKE '%retargeting%' THEN 'Retargeting'
        WHEN LOWER(bas.campaign_name) LIKE '%prospecting%' THEN 'Prospecting'
        WHEN LOWER(bas.campaign_name) LIKE '%boosted%' THEN 'Boosted Post'
        ELSE 'Other'
    END AS campaign_type,
    'Facebook' AS platform
FROM {{ ref('stg__facebook_ads__basic_ad_set') }} bas
LEFT JOIN {{ ref('stg__facebook_ads__basic_campaign') }} AS bc
    ON bc.campaign_name = bas.campaign_name