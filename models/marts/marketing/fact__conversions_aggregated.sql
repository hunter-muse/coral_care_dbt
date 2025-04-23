WITH daily_conversions AS (
    SELECT
        date_day,
        campaign_id,
        campaign_name,
        website_destination,
        campaign_type,
        platform,
        COALESCE(purchases, 0) AS purchases,
        COALESCE(landing_page_views, 0) AS landing_page_views,
        COALESCE(leads, 0) AS leads,
        cost
    FROM {{ ref('fact__conversions') }}
)

SELECT
    -- Time dimensions
    DATE_TRUNC('month', date_day) AS month,
    DATE_TRUNC('week', date_day) AS week,
    
    -- Campaign dimensions
    campaign_id,
    campaign_name,
    website_destination,
    campaign_type,
    platform,
    
    -- Conversion metrics
    SUM(purchases) AS total_purchases,
    SUM(landing_page_views) AS total_landing_page_views,
    SUM(leads) AS total_leads,
    SUM(cost) AS total_cost,
    
    -- Efficiency metrics
    SUM(cost) / NULLIF(SUM(purchases), 0) AS cost_per_purchase,
    SUM(cost) / NULLIF(SUM(leads), 0) AS cost_per_lead,
    SUM(cost) / NULLIF(SUM(landing_page_views), 0) AS cost_per_landing_page_view,
    
    -- Conversion rates
    SUM(purchases) / NULLIF(SUM(landing_page_views), 0) AS purchase_conversion_rate,
    SUM(leads) / NULLIF(SUM(landing_page_views), 0) AS lead_conversion_rate,
    
    -- Campaign activity
    COUNT(DISTINCT date_day) AS active_days,
    MIN(date_day) AS first_date,
    MAX(date_day) AS last_date
FROM daily_conversions
GROUP BY 1, 2, 3, 4, 5, 6, 7
