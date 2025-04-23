WITH daily_data AS (
    SELECT
        date_day,
        platform,
        website_destination,
        campaign_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(cost) AS cost,
        SUM(COALESCE(conversions, 0)) AS conversions
    FROM {{ ref('fact__campaign_performance') }}
    GROUP BY 1, 2, 3, 4
)

SELECT
    'Weekly' AS time_grain,
    DATE_TRUNC('week', date_day) AS time_period,
    platform,
    website_destination,
    campaign_type,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(cost) AS cost,
    SUM(conversions) AS conversions,
    SUM(clicks) / NULLIF(SUM(impressions), 0) AS ctr,
    SUM(cost) / NULLIF(SUM(clicks), 0) AS cpc,
    SUM(cost) / NULLIF(SUM(impressions), 0) * 1000 AS cpm,
    SUM(cost) / NULLIF(SUM(conversions), 0) AS cost_per_conversion
FROM daily_data
GROUP BY 1, 2, 3, 4, 5

UNION ALL

SELECT
    'Monthly' AS time_grain,
    DATE_TRUNC('month', date_day) AS time_period,
    platform,
    website_destination,
    campaign_type,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(cost) AS cost,
    SUM(conversions) AS conversions,
    SUM(clicks) / NULLIF(SUM(impressions), 0) AS ctr,
    SUM(cost) / NULLIF(SUM(clicks), 0) AS cpc,
    SUM(cost) / NULLIF(SUM(impressions), 0) * 1000 AS cpm,
    SUM(cost) / NULLIF(SUM(conversions), 0) AS cost_per_conversion
FROM daily_data
GROUP BY 1, 2, 3, 4, 5