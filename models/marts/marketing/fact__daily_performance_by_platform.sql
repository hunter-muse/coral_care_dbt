SELECT
    date_day,
    platform,
    website_destination,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(cost) AS cost,
    SUM(clicks) / NULLIF(SUM(impressions), 0) AS ctr,
    SUM(cost) / NULLIF(SUM(clicks), 0) AS cpc,
    SUM(cost) / NULLIF(SUM(impressions), 0) * 1000 AS cpm
FROM {{ ref('fact__campaign_performance') }}
GROUP BY 1, 2, 3