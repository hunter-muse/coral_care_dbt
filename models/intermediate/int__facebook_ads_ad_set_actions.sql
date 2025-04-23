-- models/intermediate/facebook_ads/int_facebook_ads_ad_set_actions.sql
WITH actions AS (
    SELECT 
        date AS date_day,
        adset_id AS ad_set_id,
        action_type,
        value AS action_value,
        -- Parse attribution windows from column names
        CASE 
            WHEN _1_d_view = TRUE THEN '1 Day View'
            WHEN _7_d_click = TRUE THEN '7 Day Click'
            ELSE 'Other'
        END AS attribution_window
    FROM {{ ref('stg__facebook_ads__basic_ad_set_actions') }}
),

-- Pivot the action types to columns with priority to 7-day click attribution
pivoted_actions AS (
    SELECT
        date_day,
        ad_set_id,
        attribution_window,
        MAX(CASE WHEN action_type = 'purchase' THEN action_value ELSE 0 END) AS purchases,
        MAX(CASE WHEN action_type = 'landing_page_view' THEN action_value ELSE 0 END) AS landing_page_views,
        MAX(CASE WHEN action_type = 'lead' THEN action_value ELSE 0 END) AS leads,
        MAX(CASE WHEN action_type = 'link_click' THEN action_value ELSE 0 END) AS link_clicks,
        MAX(CASE WHEN action_type = 'view_content' THEN action_value ELSE 0 END) AS content_views,
        ROW_NUMBER() OVER (PARTITION BY date_day, ad_set_id ORDER BY 
                          CASE WHEN attribution_window = '7 Day Click' THEN 1
                               WHEN attribution_window = '1 Day View' THEN 2
                               ELSE 3 END) AS priority
    FROM actions
    GROUP BY 1, 2, 3
)

SELECT
    date_day,
    ad_set_id,
    attribution_window,
    purchases,
    landing_page_views,
    leads,
    link_clicks,
    content_views
FROM pivoted_actions
WHERE priority = 1  -- Take the best attribution window per day/ad set