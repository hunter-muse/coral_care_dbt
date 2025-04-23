-- models/intermediate/google_ads/int_google_ads_campaigns_daily.sql
WITH campaign_history AS (
    SELECT
        id AS campaign_id,
        name AS campaign_name,
        advertising_channel_type,
        advertising_channel_subtype,
        status,
        start_date,
        end_date,
        -- Get the most recent campaign record
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _fivetran_start DESC) AS rn
    FROM {{ ref('stg__google_ads__campaign_history') }}
    WHERE _fivetran_active = TRUE
),

campaign_stats AS (
    SELECT
        date,
        id AS campaign_id,
        impressions,
        clicks,
        conversions,
        conversions_value,
        cost_micros / 1000000 AS cost, -- Convert micros to actual currency
        device,
        ad_network_type,
        active_view_impressions,
        active_view_viewability,
        view_through_conversions
    FROM {{ ref('stg__google_ads__campaign_stats') }}
)

SELECT
    cs.date AS date_day,
    cs.campaign_id,
    ch.campaign_name,
    ch.advertising_channel_type,
    ch.advertising_channel_subtype,
    ch.status,
    cs.impressions,
    cs.clicks,
    cs.cost,
    cs.conversions,
    cs.conversions_value,
    cs.view_through_conversions,
    cs.active_view_impressions,
    cs.active_view_viewability,
    cs.device,
    cs.ad_network_type,
    -- Calculated metrics for comparison with Facebook
    cs.clicks / NULLIF(cs.impressions, 0) AS ctr,
    cs.cost / NULLIF(cs.clicks, 0) AS cpc,
    cs.cost / NULLIF(cs.impressions, 0) * 1000 AS cpm,
    cs.cost / NULLIF(cs.conversions, 0) AS cost_per_conversion,
    -- Website classification
    CASE 
        WHEN LOWER(ch.campaign_name) LIKE '%joincoralcare%' THEN 'JoinCoralCare'
        WHEN LOWER(ch.campaign_name) LIKE '%provider%' THEN 'Provider Microsite'
        ELSE 'Other'
    END AS website_destination,
    -- Campaign purpose classification
    CASE
        WHEN LOWER(ch.campaign_name) LIKE '%remarketing%' OR LOWER(ch.campaign_name) LIKE '%retargeting%' THEN 'Retargeting'
        WHEN LOWER(ch.campaign_name) LIKE '%brand%' THEN 'Brand'
        WHEN LOWER(ch.campaign_name) LIKE '%non-brand%' OR LOWER(ch.campaign_name) LIKE '%nonbrand%' THEN 'Non-Brand'
        WHEN LOWER(ch.campaign_name) LIKE '%display%' THEN 'Display'
        ELSE 'Other'
    END AS campaign_type,
    'Google' AS platform
FROM campaign_stats cs
JOIN campaign_history ch ON cs.campaign_id = ch.campaign_id
WHERE ch.rn = 1 -- Get only the most recent campaign data