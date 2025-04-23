-- models/intermediate/google_ads/int_google_ads_ad_groups_daily.sql
WITH ad_group_history AS (
    SELECT 
        id AS ad_group_id,
        name AS ad_group_name,
        campaign_id,
        status,
        type,
        -- Get the most recent ad group record
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    FROM {{ ref('stg__google_ads__ad_group_history') }}
    WHERE _fivetran_active = TRUE
),

ad_group_stats AS (
    SELECT 
        date,
        id AS ad_group_id,
        campaign_id,
        impressions,
        clicks,
        cost_micros / 1000000 AS cost, -- Convert micros to actual currency
        conversions,
        conversions_value,
        device,
        ad_network_type,
        active_view_impressions,
        active_view_viewability,
        view_through_conversions
    FROM {{ ref('stg__google_ads__ad_group_stats') }}
)

SELECT
    ags.date AS date_day,
    ags.ad_group_id,
    agh.ad_group_name,
    ags.campaign_id,
    agh.status,
    agh.type,
    ags.impressions,
    ags.clicks,
    ags.cost,
    ags.conversions,
    ags.conversions_value,
    ags.view_through_conversions,
    ags.active_view_impressions,
    ags.active_view_viewability,
    ags.device,
    ags.ad_network_type,
    -- Calculated metrics for comparison with Facebook
    ags.clicks / NULLIF(ags.impressions, 0) AS ctr,
    ags.cost / NULLIF(ags.clicks, 0) AS cpc,
    ags.cost / NULLIF(ags.impressions, 0) * 1000 AS cpm,
    ags.cost / NULLIF(ags.conversions, 0) AS cost_per_conversion,
    'Google' AS platform
FROM ad_group_stats ags
JOIN ad_group_history agh ON ags.ad_group_id = agh.ad_group_id
WHERE agh.rn = 1 -- Get only the most recent ad group data