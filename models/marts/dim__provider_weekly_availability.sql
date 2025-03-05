{{ config(
    materialized='table'
) }}

WITH weekly_availability AS (
    SELECT 
        provider_detail,
        coral_provider_id,
        DATE_TRUNC('week', calendar_date) AS week_start_date,
        DATEADD('day', 6, week_start_date) AS week_end_date,
        MIN(time_period) AS time_period,
        week_number,
        
        -- Daily base hours (total potential hours)
        SUM(CASE WHEN day_of_week = 'Mon' THEN total_available_hours ELSE 0 END) AS monday_total_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN total_available_hours ELSE 0 END) AS tuesday_total_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN total_available_hours ELSE 0 END) AS wednesday_total_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN total_available_hours ELSE 0 END) AS thursday_total_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN total_available_hours ELSE 0 END) AS friday_total_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN total_available_hours ELSE 0 END) AS saturday_total_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN total_available_hours ELSE 0 END) AS sunday_total_hours,
        
        -- Daily busy hours
        SUM(CASE WHEN day_of_week = 'Mon' THEN total_busy_hours ELSE 0 END) AS monday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN total_busy_hours ELSE 0 END) AS tuesday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN total_busy_hours ELSE 0 END) AS wednesday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN total_busy_hours ELSE 0 END) AS thursday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN total_busy_hours ELSE 0 END) AS friday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN total_busy_hours ELSE 0 END) AS saturday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN total_busy_hours ELSE 0 END) AS sunday_busy_hours,
        
        -- Daily available hours (after accounting for busy slots)
        SUM(CASE WHEN day_of_week = 'Mon' THEN net_available_hours ELSE 0 END) AS monday_available_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN net_available_hours ELSE 0 END) AS tuesday_available_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN net_available_hours ELSE 0 END) AS wednesday_available_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN net_available_hours ELSE 0 END) AS thursday_available_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN net_available_hours ELSE 0 END) AS friday_available_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN net_available_hours ELSE 0 END) AS saturday_available_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN net_available_hours ELSE 0 END) AS sunday_available_hours,
        
        -- Weekly totals
        ROUND(SUM(total_available_hours), 2) AS total_weekly_hours,
        ROUND(SUM(total_busy_hours), 2) AS total_weekly_busy_hours,
        ROUND(SUM(net_available_hours), 2) AS total_weekly_available_hours,
        
        -- Morning segments (using net_morning_hours which accounts for busy time)
        SUM(CASE WHEN day_of_week = 'Mon' THEN net_morning_hours ELSE 0 END) AS monday_morning_available_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN net_morning_hours ELSE 0 END) AS tuesday_morning_available_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN net_morning_hours ELSE 0 END) AS wednesday_morning_available_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN net_morning_hours ELSE 0 END) AS thursday_morning_available_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN net_morning_hours ELSE 0 END) AS friday_morning_available_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN net_morning_hours ELSE 0 END) AS saturday_morning_available_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN net_morning_hours ELSE 0 END) AS sunday_morning_available_hours,
        
        -- Afternoon segments
        SUM(CASE WHEN day_of_week = 'Mon' THEN net_afternoon_hours ELSE 0 END) AS monday_afternoon_available_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN net_afternoon_hours ELSE 0 END) AS tuesday_afternoon_available_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN net_afternoon_hours ELSE 0 END) AS wednesday_afternoon_available_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN net_afternoon_hours ELSE 0 END) AS thursday_afternoon_available_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN net_afternoon_hours ELSE 0 END) AS friday_afternoon_available_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN net_afternoon_hours ELSE 0 END) AS saturday_afternoon_available_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN net_afternoon_hours ELSE 0 END) AS sunday_afternoon_available_hours,
        
        -- Evening segments
        SUM(CASE WHEN day_of_week = 'Mon' THEN net_evening_hours ELSE 0 END) AS monday_evening_available_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN net_evening_hours ELSE 0 END) AS tuesday_evening_available_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN net_evening_hours ELSE 0 END) AS wednesday_evening_available_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN net_evening_hours ELSE 0 END) AS thursday_evening_available_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN net_evening_hours ELSE 0 END) AS friday_evening_available_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN net_evening_hours ELSE 0 END) AS saturday_evening_available_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN net_evening_hours ELSE 0 END) AS sunday_evening_available_hours
    FROM {{ ref('int__provider_daily_availability') }}
    GROUP BY 
        provider_detail,
        coral_provider_id,
        week_start_date,
        week_end_date,
        week_number
)

SELECT 
    provider_detail,
    coral_provider_id,
    week_start_date,
    week_end_date,
    time_period,
    week_number,
    
    -- Daily base hours (potential hours)
    ROUND(monday_total_hours, 2) AS monday_total_hours,
    ROUND(tuesday_total_hours, 2) AS tuesday_total_hours,
    ROUND(wednesday_total_hours, 2) AS wednesday_total_hours,
    ROUND(thursday_total_hours, 2) AS thursday_total_hours,
    ROUND(friday_total_hours, 2) AS friday_total_hours,
    ROUND(saturday_total_hours, 2) AS saturday_total_hours,
    ROUND(sunday_total_hours, 2) AS sunday_total_hours,
    
    -- Daily busy hours (blocked by appointments/unavailability)
    ROUND(monday_busy_hours, 2) AS monday_busy_hours,
    ROUND(tuesday_busy_hours, 2) AS tuesday_busy_hours,
    ROUND(wednesday_busy_hours, 2) AS wednesday_busy_hours,
    ROUND(thursday_busy_hours, 2) AS thursday_busy_hours,
    ROUND(friday_busy_hours, 2) AS friday_busy_hours,
    ROUND(saturday_busy_hours, 2) AS saturday_busy_hours,
    ROUND(sunday_busy_hours, 2) AS sunday_busy_hours,
    
    -- Daily available hours
    ROUND(monday_available_hours, 2) AS monday_available_hours,
    ROUND(tuesday_available_hours, 2) AS tuesday_available_hours,
    ROUND(wednesday_available_hours, 2) AS wednesday_available_hours,
    ROUND(thursday_available_hours, 2) AS thursday_available_hours,
    ROUND(friday_available_hours, 2) AS friday_available_hours,
    ROUND(saturday_available_hours, 2) AS saturday_available_hours,
    ROUND(sunday_available_hours, 2) AS sunday_available_hours,
    
    -- Weekly totals
    total_weekly_hours,
    total_weekly_busy_hours,
    total_weekly_available_hours,
    
    -- Utilization rate (percentage of hours blocked)
    CASE WHEN total_weekly_hours > 0 
        THEN ROUND((total_weekly_busy_hours / total_weekly_hours) * 100, 1)
        ELSE 0 
    END AS utilization_rate,
    
    -- Daily segments (for available hours)
    ROUND(monday_morning_available_hours, 2) AS monday_morning_available_hours,
    ROUND(monday_afternoon_available_hours, 2) AS monday_afternoon_available_hours,
    ROUND(monday_evening_available_hours, 2) AS monday_evening_available_hours,
    ROUND(tuesday_morning_available_hours, 2) AS tuesday_morning_available_hours,
    ROUND(tuesday_afternoon_available_hours, 2) AS tuesday_afternoon_available_hours,
    ROUND(tuesday_evening_available_hours, 2) AS tuesday_evening_available_hours,
    ROUND(wednesday_morning_available_hours, 2) AS wednesday_morning_available_hours,
    ROUND(wednesday_afternoon_available_hours, 2) AS wednesday_afternoon_available_hours,
    ROUND(wednesday_evening_available_hours, 2) AS wednesday_evening_available_hours,
    ROUND(thursday_morning_available_hours, 2) AS thursday_morning_available_hours,
    ROUND(thursday_afternoon_available_hours, 2) AS thursday_afternoon_available_hours,
    ROUND(thursday_evening_available_hours, 2) AS thursday_evening_available_hours,
    ROUND(friday_morning_available_hours, 2) AS friday_morning_available_hours,
    ROUND(friday_afternoon_available_hours, 2) AS friday_afternoon_available_hours,
    ROUND(friday_evening_available_hours, 2) AS friday_evening_available_hours,
    ROUND(saturday_morning_available_hours, 2) AS saturday_morning_available_hours,
    ROUND(saturday_afternoon_available_hours, 2) AS saturday_afternoon_available_hours,
    ROUND(saturday_evening_available_hours, 2) AS saturday_evening_available_hours,
    ROUND(sunday_morning_available_hours, 2) AS sunday_morning_available_hours,
    ROUND(sunday_afternoon_available_hours, 2) AS sunday_afternoon_available_hours,
    ROUND(sunday_evening_available_hours, 2) AS sunday_evening_available_hours,
    
    -- Weekly segments (sum of daily segments)
    ROUND((monday_morning_available_hours + tuesday_morning_available_hours + wednesday_morning_available_hours + 
     thursday_morning_available_hours + friday_morning_available_hours + saturday_morning_available_hours + 
     sunday_morning_available_hours), 2) AS weekly_morning_available_hours,
    
    ROUND((monday_afternoon_available_hours + tuesday_afternoon_available_hours + wednesday_afternoon_available_hours + 
     thursday_afternoon_available_hours + friday_afternoon_available_hours + saturday_afternoon_available_hours + 
     sunday_afternoon_available_hours), 2) AS weekly_afternoon_available_hours,
    
    ROUND((monday_evening_available_hours + tuesday_evening_available_hours + wednesday_evening_available_hours + 
     thursday_evening_available_hours + friday_evening_available_hours + saturday_evening_available_hours + 
     sunday_evening_available_hours), 2) AS weekly_evening_available_hours,
    
    -- Available weekdays and weekend days
    (CASE WHEN monday_available_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN tuesday_available_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN wednesday_available_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN thursday_available_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN friday_available_hours > 0 THEN 1 ELSE 0 END) AS weekday_availability_count,
     
    (CASE WHEN saturday_available_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN sunday_available_hours > 0 THEN 1 ELSE 0 END) AS weekend_availability_count
FROM weekly_availability
WHERE week_start_date >= DATE_TRUNC('week', CURRENT_DATE())
  AND week_start_date <= DATEADD('week', 4, DATE_TRUNC('week', CURRENT_DATE()))
ORDER BY 
    provider_detail,
    week_start_date