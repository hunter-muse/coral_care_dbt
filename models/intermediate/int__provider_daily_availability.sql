WITH date_spine AS (
    -- This CTE creates a date spine from the start of current week to 4 weeks in the future
    SELECT 
        DATEADD('day', ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, DATE_TRUNC('week', CURRENT_DATE())) AS calendar_date,
        DAYNAME(calendar_date) AS day_of_week,
        CASE 
            WHEN calendar_date BETWEEN DATE_TRUNC('week', CURRENT_DATE()) AND DATEADD('day', 6, DATE_TRUNC('week', CURRENT_DATE())) 
            THEN 'Current Week'
            WHEN calendar_date BETWEEN DATEADD('week', 1, DATE_TRUNC('week', CURRENT_DATE())) AND DATEADD('day', 6, DATEADD('week', 1, DATE_TRUNC('week', CURRENT_DATE())))
            THEN 'Next Week'
            ELSE 'Future Weeks'
        END AS time_period,
        WEEKOFYEAR(calendar_date) AS week_number
    FROM TABLE(GENERATOR(ROWCOUNT => 35)) -- Full current week + 28 days (4 weeks)
),

provider_base_availability AS (
    -- Use the int__provider_availability model which has already aggregated by day of week
    SELECT 
        provider_detail,
        coral_provider_id,
        -- Extract daily hours by day of week
        monday_total_hours,
        tuesday_total_hours,
        wednesday_total_hours,
        thursday_total_hours,
        friday_total_hours,
        saturday_total_hours,
        sunday_total_hours,
        
        -- Extract daily segments
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
        
        -- Extract daily time slots
        monday_availability,
        tuesday_availability,
        wednesday_availability,
        thursday_availability,
        friday_availability,
        saturday_availability,
        sunday_availability
    FROM {{ref('int__provider_availability')}}
),

provider_days AS (
    -- Join provider availability with date spine
    SELECT 
        p.provider_detail,
        p.coral_provider_id,
        d.calendar_date,
        d.day_of_week,
        d.time_period,
        d.week_number,
        CASE 
            WHEN d.day_of_week = 'Mon' THEN p.monday_total_hours
            WHEN d.day_of_week = 'Tue' THEN p.tuesday_total_hours
            WHEN d.day_of_week = 'Wed' THEN p.wednesday_total_hours
            WHEN d.day_of_week = 'Thu' THEN p.thursday_total_hours
            WHEN d.day_of_week = 'Fri' THEN p.friday_total_hours
            WHEN d.day_of_week = 'Sat' THEN p.saturday_total_hours
            WHEN d.day_of_week = 'Sun' THEN p.sunday_total_hours
            ELSE 0
        END AS total_available_hours,
        
        -- Morning hours by day
        CASE 
            WHEN d.day_of_week = 'Mon' THEN p.monday_morning_hours
            WHEN d.day_of_week = 'Tue' THEN p.tuesday_morning_hours
            WHEN d.day_of_week = 'Wed' THEN p.wednesday_morning_hours
            WHEN d.day_of_week = 'Thu' THEN p.thursday_morning_hours
            WHEN d.day_of_week = 'Fri' THEN p.friday_morning_hours
            WHEN d.day_of_week = 'Sat' THEN p.saturday_morning_hours
            WHEN d.day_of_week = 'Sun' THEN p.sunday_morning_hours
            ELSE 0
        END AS morning_hours,
        
        -- Afternoon hours by day
        CASE 
            WHEN d.day_of_week = 'Mon' THEN p.monday_afternoon_hours
            WHEN d.day_of_week = 'Tue' THEN p.tuesday_afternoon_hours
            WHEN d.day_of_week = 'Wed' THEN p.wednesday_afternoon_hours
            WHEN d.day_of_week = 'Thu' THEN p.thursday_afternoon_hours
            WHEN d.day_of_week = 'Fri' THEN p.friday_afternoon_hours
            WHEN d.day_of_week = 'Sat' THEN p.saturday_afternoon_hours
            WHEN d.day_of_week = 'Sun' THEN p.sunday_afternoon_hours
            ELSE 0
        END AS afternoon_hours,
        
        -- Evening hours by day
        CASE 
            WHEN d.day_of_week = 'Mon' THEN p.monday_evening_hours
            WHEN d.day_of_week = 'Tue' THEN p.tuesday_evening_hours
            WHEN d.day_of_week = 'Wed' THEN p.wednesday_evening_hours
            WHEN d.day_of_week = 'Thu' THEN p.thursday_evening_hours
            WHEN d.day_of_week = 'Fri' THEN p.friday_evening_hours
            WHEN d.day_of_week = 'Sat' THEN p.saturday_evening_hours
            WHEN d.day_of_week = 'Sun' THEN p.sunday_evening_hours
            ELSE 0
        END AS evening_hours,
        
        -- Time slots by day
        CASE 
            WHEN d.day_of_week = 'Mon' THEN p.monday_availability
            WHEN d.day_of_week = 'Tue' THEN p.tuesday_availability
            WHEN d.day_of_week = 'Wed' THEN p.wednesday_availability
            WHEN d.day_of_week = 'Thu' THEN p.thursday_availability
            WHEN d.day_of_week = 'Fri' THEN p.friday_availability
            WHEN d.day_of_week = 'Sat' THEN p.saturday_availability
            WHEN d.day_of_week = 'Sun' THEN p.sunday_availability
            ELSE NULL
        END AS time_slot
    FROM provider_base_availability p
    CROSS JOIN date_spine d
),

busy_slot_segments AS (
    -- Process busy slots and categorize them into morning/afternoon/evening
    SELECT 
        provider_detail,
        DATE(start_date) AS busy_date,
        -- Categorize each busy slot into time segments
        SUM(CASE 
            WHEN HOUR(start_date) >= 5 AND HOUR(start_date) < 12 THEN 
                DATEDIFF('hour', 
                    GREATEST(start_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 05:00:00')), 
                    LEAST(end_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 12:00:00'))
                )
            ELSE 0 
        END) AS morning_busy_hours,
        
        SUM(CASE 
            WHEN HOUR(start_date) >= 12 AND HOUR(start_date) < 17 THEN 
                DATEDIFF('hour', 
                    GREATEST(start_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 12:00:00')), 
                    LEAST(end_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 17:00:00'))
                )
            ELSE 0 
        END) AS afternoon_busy_hours,
        
        SUM(CASE 
            WHEN HOUR(start_date) >= 17 AND HOUR(start_date) < 22 THEN 
                DATEDIFF('hour', 
                    GREATEST(start_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 17:00:00')), 
                    LEAST(end_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 22:00:00'))
                )
            ELSE 0 
        END) AS evening_busy_hours,
        
        -- Total busy hours (may include hours outside the defined segments)
        SUM(DATEDIFF('hour', start_date, end_date)) AS total_busy_hours
    FROM {{ref('stg__bubble__busyslot')}}
    WHERE 
        delete_flag = 'false'
        AND DATE(start_date) >= DATE_TRUNC('week', CURRENT_DATE())
        AND DATE(start_date) <= DATEADD('day', 28, CURRENT_DATE())
    GROUP BY 
        provider_detail,
        busy_date
)

-- Final output
SELECT 
    pd.provider_detail,
    pd.coral_provider_id,
    pd.calendar_date,
    pd.day_of_week,
    pd.week_number,
    pd.time_period,
    pd.time_slot,
    pd.total_available_hours,
    COALESCE(bs.total_busy_hours, 0) AS total_busy_hours,
    GREATEST(pd.total_available_hours - COALESCE(bs.total_busy_hours, 0), 0) AS net_available_hours,
    
    -- Original segment hours (from template)
    pd.morning_hours,
    pd.afternoon_hours,
    pd.evening_hours,
    
    -- Busy hours by segment
    COALESCE(bs.morning_busy_hours, 0) AS morning_busy_hours,
    COALESCE(bs.afternoon_busy_hours, 0) AS afternoon_busy_hours,
    COALESCE(bs.evening_busy_hours, 0) AS evening_busy_hours,
    
    -- Net available hours by segment
    GREATEST(pd.morning_hours - COALESCE(bs.morning_busy_hours, 0), 0) AS net_morning_hours,
    GREATEST(pd.afternoon_hours - COALESCE(bs.afternoon_busy_hours, 0), 0) AS net_afternoon_hours,
    GREATEST(pd.evening_hours - COALESCE(bs.evening_busy_hours, 0), 0) AS net_evening_hours
FROM provider_days pd
LEFT JOIN busy_slot_segments bs ON 
    pd.provider_detail = bs.provider_detail 
    AND pd.calendar_date = bs.busy_date
ORDER BY 
    pd.provider_detail, 
    pd.calendar_date