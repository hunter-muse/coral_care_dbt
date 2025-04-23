SELECT
    campaign_id,
    campaign_name,
    website_destination,
    campaign_type,
    platform,
    SUM(cost) AS total_cost,
    SUM(purchases) AS total_purchases,
    SUM(leads) AS total_leads,
    SUM(landing_page_views) AS total_landing_page_views,
    
    -- Efficiency metrics
    SUM(cost) / NULLIF(SUM(purchases), 0) AS cost_per_purchase,
    SUM(cost) / NULLIF(SUM(leads), 0) AS cost_per_lead,
    
    -- Campaign activity
    MIN(date_day) AS first_date,
    MAX(date_day) AS last_date,
    COUNT(DISTINCT date_day) AS active_days,
    
    -- Performance ranking
    ROW_NUMBER() OVER (PARTITION BY platform ORDER BY SUM(purchases) DESC) AS purchase_rank,
    ROW_NUMBER() OVER (PARTITION BY platform ORDER BY SUM(leads) DESC) AS lead_rank,
    ROW_NUMBER() OVER (PARTITION BY platform ORDER BY SUM(cost) / NULLIF(SUM(purchases), 0)) AS efficiency_rank
FROM {{ ref('fact__conversions') }}
GROUP BY 1, 2, 3, 4, 5