SELECT
    platform,
    website_destination,
    campaign_type,
    DATE_TRUNC('month', date_day) AS month,
    SUM(cost) AS total_cost,
    SUM(purchases) AS total_purchases,
    SUM(leads) AS total_leads,
    SUM(landing_page_views) AS total_landing_page_views,
    
    -- Efficiency metrics
    SUM(cost) / NULLIF(SUM(purchases), 0) AS cost_per_purchase,
    SUM(cost) / NULLIF(SUM(leads), 0) AS cost_per_lead,
    SUM(cost) / NULLIF(SUM(landing_page_views), 0) AS cost_per_landing_page_view
FROM {{ ref('fact__conversions') }}
GROUP BY 1, 2, 3, 4