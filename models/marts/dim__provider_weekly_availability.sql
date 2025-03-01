WITH weekly_availability AS (
    SELECT 
        provider_detail,
        coral_provider_id,
        DATE_TRUNC('week', calendar_date) AS week_start_date,
        DATEADD('day', 6, week_start_date) AS week_end_date,
        MIN(time_period) AS time_period,  -- Use the earliest period in the week
        week_number,
        -- Daily availability hours - use net_available_hours which accounts for busy slots
        SUM(CASE WHEN day_of_week = 'Mon' THEN net_available_hours ELSE 0 END) AS monday_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN net_available_hours ELSE 0 END) AS tuesday_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN net_available_hours ELSE 0 END) AS wednesday_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN net_available_hours ELSE 0 END) AS thursday_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN net_available_hours ELSE 0 END) AS friday_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN net_available_hours ELSE 0 END) AS saturday_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN net_available_hours ELSE 0 END) AS sunday_hours,
        
        -- Total weekly hours
        SUM(net_available_hours) AS total_weekly_hours,
        
        -- Daily time segments considering busy slots
        -- Morning hours (8AM-12PM)
        SUM(CASE WHEN day_of_week = 'Mon' THEN 
                GREATEST(morning_hours - 
                    CASE
                        WHEN total_busy_hours > 0 THEN 
                            LEAST(morning_hours, total_busy_hours)
                        ELSE 0
                    END, 0)
            ELSE 0 END) AS monday_morning_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN 
                GREATEST(morning_hours - 
                    CASE
                        WHEN total_busy_hours > 0 THEN 
                            LEAST(morning_hours, total_busy_hours)
                        ELSE 0
                    END, 0)
            ELSE 0 END) AS tuesday_morning_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN 
                GREATEST(morning_hours - 
                    CASE
                        WHEN total_busy_hours > 0 THEN 
                            LEAST(morning_hours, total_busy_hours)
                        ELSE 0
                    END, 0)
            ELSE 0 END) AS wednesday_morning_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN 
                GREATEST(morning_hours - 
                    CASE
                        WHEN total_busy_hours > 0 THEN 
                            LEAST(morning_hours, total_busy_hours)
                        ELSE 0
                    END, 0)
            ELSE 0 END) AS thursday_morning_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN 
                GREATEST(morning_hours - 
                    CASE
                        WHEN total_busy_hours > 0 THEN 
                            LEAST(morning_hours, total_busy_hours)
                        ELSE 0
                    END, 0)
            ELSE 0 END) AS friday_morning_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN 
                GREATEST(morning_hours - 
                    CASE
                        WHEN total_busy_hours > 0 THEN 
                            LEAST(morning_hours, total_busy_hours)
                        ELSE 0
                    END, 0)
            ELSE 0 END) AS saturday_morning_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN 
                GREATEST(morning_hours - 
                    CASE
                        WHEN total_busy_hours > 0 THEN 
                            LEAST(morning_hours, total_busy_hours)
                        ELSE 0
                    END, 0)
            ELSE 0 END) AS sunday_morning_hours,
        
        -- Afternoon hours (12PM-5PM) - simplified approach for afternoon hours
        SUM(CASE WHEN day_of_week = 'Mon' THEN afternoon_hours ELSE 0 END) AS monday_afternoon_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN afternoon_hours ELSE 0 END) AS tuesday_afternoon_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN afternoon_hours ELSE 0 END) AS wednesday_afternoon_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN afternoon_hours ELSE 0 END) AS thursday_afternoon_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN afternoon_hours ELSE 0 END) AS friday_afternoon_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN afternoon_hours ELSE 0 END) AS saturday_afternoon_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN afternoon_hours ELSE 0 END) AS sunday_afternoon_hours,
        
        -- Evening hours (5PM onwards) - simplified approach for evening hours
        SUM(CASE WHEN day_of_week = 'Mon' THEN evening_hours ELSE 0 END) AS monday_evening_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN evening_hours ELSE 0 END) AS tuesday_evening_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN evening_hours ELSE 0 END) AS wednesday_evening_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN evening_hours ELSE 0 END) AS thursday_evening_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN evening_hours ELSE 0 END) AS friday_evening_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN evening_hours ELSE 0 END) AS saturday_evening_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN evening_hours ELSE 0 END) AS sunday_evening_hours,
        
        -- Weekly segment hours - summing up the daily segments
        (monday_morning_hours + tuesday_morning_hours + wednesday_morning_hours + 
         thursday_morning_hours + friday_morning_hours + saturday_morning_hours + 
         sunday_morning_hours) AS weekly_morning_hours,
        
        (monday_afternoon_hours + tuesday_afternoon_hours + wednesday_afternoon_hours + 
         thursday_afternoon_hours + friday_afternoon_hours + saturday_afternoon_hours + 
         sunday_afternoon_hours) AS weekly_afternoon_hours,
        
        (monday_evening_hours + tuesday_evening_hours + wednesday_evening_hours + 
         thursday_evening_hours + friday_evening_hours + saturday_evening_hours + 
         sunday_evening_hours) AS weekly_evening_hours
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
    -- Daily hours
    monday_hours,
    tuesday_hours,
    wednesday_hours,
    thursday_hours,
    friday_hours,
    saturday_hours,
    sunday_hours,
    
    -- Weekly total
    total_weekly_hours,
    
    -- Daily segments
    monday_morning_hours,
    monday_afternoon_hours,
    monday_evening_hours,
    tuesday_morning_hours,
    tuesday_afternoon_hours,
    tuesday_evening_hours,
    wednesday_morning_hours,
    wednesday_afternoon_hours,
    wednesday_evening_hours,
    thursday_morning_hours,
    thursday_afternoon_hours,
    thursday_evening_hours,
    friday_morning_hours,
    friday_afternoon_hours,
    friday_evening_hours,
    saturday_morning_hours,
    saturday_afternoon_hours,
    saturday_evening_hours,
    sunday_morning_hours,
    sunday_afternoon_hours,
    sunday_evening_hours,
    
    -- Weekly segments
    weekly_morning_hours,
    weekly_afternoon_hours,
    weekly_evening_hours,
    
    -- Available weekdays and weekend days
    (CASE WHEN monday_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN tuesday_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN wednesday_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN thursday_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN friday_hours > 0 THEN 1 ELSE 0 END) AS weekday_availability_count,
     
    (CASE WHEN saturday_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN sunday_hours > 0 THEN 1 ELSE 0 END) AS weekend_availability_count
FROM weekly_availability
WHERE week_start_date >= DATE_TRUNC('week', CURRENT_DATE())
  AND week_start_date <= DATEADD('week', 4, DATE_TRUNC('week', CURRENT_DATE()))
ORDER BY 
    provider_detail,
    week_start_date