WITH daily_aggregated AS (
    -- First aggregate to the day level to handle multiple slots per day
    SELECT 
        provider_detail,
        coral_provider_id,
        calendar_date,
        day_of_week,
        time_period,
        week_number,
        
        -- Sum up hours across all slots for each day
        SUM(total_available_hours) AS total_available_hours,
        SUM(net_available_hours) AS net_available_hours,
        SUM(morning_hours) AS morning_hours,
        SUM(afternoon_hours) AS afternoon_hours,
        SUM(evening_hours) AS evening_hours,
        MAX(total_busy_hours) AS total_busy_hours, -- Use MAX since this is the same for all slots on a day
        MAX(morning_busy_hours) AS morning_busy_hours,
        MAX(afternoon_busy_hours) AS afternoon_busy_hours,
        MAX(evening_busy_hours) AS evening_busy_hours,
        SUM(morning_overlapping_busy_hours) AS morning_overlapping_busy_hours,
        SUM(afternoon_overlapping_busy_hours) AS afternoon_overlapping_busy_hours,
        SUM(evening_overlapping_busy_hours) AS evening_overlapping_busy_hours,
        SUM(net_morning_hours) AS net_morning_hours,
        SUM(net_afternoon_hours) AS net_afternoon_hours,
        SUM(net_evening_hours) AS net_evening_hours,
        
        -- Aggregate time slots and actual availability for each day
        LISTAGG(time_slot, ', ') WITHIN GROUP (ORDER BY slot_id) AS time_slots,
        LISTAGG(actual_availability, ', ') WITHIN GROUP (ORDER BY slot_id) AS actual_availability_slots
    FROM {{ ref('int__provider_daily_availability') }}
    GROUP BY 
        provider_detail,
        coral_provider_id,
        calendar_date,
        day_of_week,
        time_period,
        week_number
),

weekly_availability AS (
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
        
        -- Daily actual availability flags
        MAX(CASE WHEN day_of_week = 'Mon' AND time_slots != '' THEN 1 ELSE 0 END) AS monday_actual_availability,
        MAX(CASE WHEN day_of_week = 'Tue' AND time_slots != '' THEN 1 ELSE 0 END) AS tuesday_actual_availability,
        MAX(CASE WHEN day_of_week = 'Wed' AND time_slots != '' THEN 1 ELSE 0 END) AS wednesday_actual_availability,
        MAX(CASE WHEN day_of_week = 'Thu' AND time_slots != '' THEN 1 ELSE 0 END) AS thursday_actual_availability,
        MAX(CASE WHEN day_of_week = 'Fri' AND time_slots != '' THEN 1 ELSE 0 END) AS friday_actual_availability,
        MAX(CASE WHEN day_of_week = 'Sat' AND time_slots != '' THEN 1 ELSE 0 END) AS saturday_actual_availability,
        MAX(CASE WHEN day_of_week = 'Sun' AND time_slots != '' THEN 1 ELSE 0 END) AS sunday_actual_availability,
        
        -- Capture the actual availability time slots for each day
        MAX(CASE WHEN day_of_week = 'Mon' THEN actual_availability_slots ELSE '' END) AS monday_actual_availability_slots,
        MAX(CASE WHEN day_of_week = 'Tue' THEN actual_availability_slots ELSE '' END) AS tuesday_actual_availability_slots,
        MAX(CASE WHEN day_of_week = 'Wed' THEN actual_availability_slots ELSE '' END) AS wednesday_actual_availability_slots,
        MAX(CASE WHEN day_of_week = 'Thu' THEN actual_availability_slots ELSE '' END) AS thursday_actual_availability_slots,
        MAX(CASE WHEN day_of_week = 'Fri' THEN actual_availability_slots ELSE '' END) AS friday_actual_availability_slots,
        MAX(CASE WHEN day_of_week = 'Sat' THEN actual_availability_slots ELSE '' END) AS saturday_actual_availability_slots,
        MAX(CASE WHEN day_of_week = 'Sun' THEN actual_availability_slots ELSE '' END) AS sunday_actual_availability_slots,
        
        -- Daily busy hours
        SUM(CASE WHEN day_of_week = 'Mon' THEN total_busy_hours ELSE 0 END) AS monday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Tue' THEN total_busy_hours ELSE 0 END) AS tuesday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Wed' THEN total_busy_hours ELSE 0 END) AS wednesday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Thu' THEN total_busy_hours ELSE 0 END) AS thursday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Fri' THEN total_busy_hours ELSE 0 END) AS friday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Sat' THEN total_busy_hours ELSE 0 END) AS saturday_busy_hours,
        SUM(CASE WHEN day_of_week = 'Sun' THEN total_busy_hours ELSE 0 END) AS sunday_busy_hours,
        
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
    FROM daily_aggregated
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
    ROUND(monday_total_hours, 2) AS monday_potential_hours,
    ROUND(tuesday_total_hours, 2) AS tuesday_potential_hours,
    ROUND(wednesday_total_hours, 2) AS wednesday_potential_hours,
    ROUND(thursday_total_hours, 2) AS thursday_potential_hours,
    ROUND(friday_total_hours, 2) AS friday_potential_hours,
    ROUND(saturday_total_hours, 2) AS saturday_potential_hours,
    ROUND(sunday_total_hours, 2) AS sunday_potential_hours,
    
    -- Daily actual availability flags
    monday_actual_availability AS is_monday_available,
    tuesday_actual_availability AS is_tuesday_available,
    wednesday_actual_availability AS is_wednesday_available,
    thursday_actual_availability AS is_thursday_available,
    friday_actual_availability AS is_friday_available,
    saturday_actual_availability AS is_saturday_available,
    sunday_actual_availability AS is_sunday_available,
    
    -- Daily actual availability time slots
    monday_actual_availability_slots AS monday_available_time_slots,
    tuesday_actual_availability_slots AS tuesday_available_time_slots,
    wednesday_actual_availability_slots AS wednesday_available_time_slots,
    thursday_actual_availability_slots AS thursday_available_time_slots,
    friday_actual_availability_slots AS friday_available_time_slots,
    saturday_actual_availability_slots AS saturday_available_time_slots,
    sunday_actual_availability_slots AS sunday_available_time_slots,
    
    -- Daily busy hours (blocked by appointments/unavailability)
    ROUND(monday_busy_hours, 2) AS monday_busy_hours,
    ROUND(tuesday_busy_hours, 2) AS tuesday_busy_hours,
    ROUND(wednesday_busy_hours, 2) AS wednesday_busy_hours,
    ROUND(thursday_busy_hours, 2) AS thursday_busy_hours,
    ROUND(friday_busy_hours, 2) AS friday_busy_hours,
    ROUND(saturday_busy_hours, 2) AS saturday_busy_hours,
    ROUND(sunday_busy_hours, 2) AS sunday_busy_hours,
    
    -- Daily available hours calculated as sum of segments
    ROUND(monday_morning_available_hours + monday_afternoon_available_hours + monday_evening_available_hours, 2) AS monday_net_available_hours,
    ROUND(tuesday_morning_available_hours + tuesday_afternoon_available_hours + tuesday_evening_available_hours, 2) AS tuesday_net_available_hours,
    ROUND(wednesday_morning_available_hours + wednesday_afternoon_available_hours + wednesday_evening_available_hours, 2) AS wednesday_net_available_hours,
    ROUND(thursday_morning_available_hours + thursday_afternoon_available_hours + thursday_evening_available_hours, 2) AS thursday_net_available_hours,
    ROUND(friday_morning_available_hours + friday_afternoon_available_hours + friday_evening_available_hours, 2) AS friday_net_available_hours,
    ROUND(saturday_morning_available_hours + saturday_afternoon_available_hours + saturday_evening_available_hours, 2) AS saturday_net_available_hours,
    ROUND(sunday_morning_available_hours + sunday_afternoon_available_hours + sunday_evening_available_hours, 2) AS sunday_net_available_hours,
    
    -- Weekly totals recalculated from daily sums
    ROUND(
        (monday_morning_available_hours + monday_afternoon_available_hours + monday_evening_available_hours) +
        (tuesday_morning_available_hours + tuesday_afternoon_available_hours + tuesday_evening_available_hours) +
        (wednesday_morning_available_hours + wednesday_afternoon_available_hours + wednesday_evening_available_hours) +
        (thursday_morning_available_hours + thursday_afternoon_available_hours + thursday_evening_available_hours) +
        (friday_morning_available_hours + friday_afternoon_available_hours + friday_evening_available_hours) +
        (saturday_morning_available_hours + saturday_afternoon_available_hours + saturday_evening_available_hours) +
        (sunday_morning_available_hours + sunday_afternoon_available_hours + sunday_evening_available_hours), 2
    ) AS total_weekly_net_available_hours,
    
    -- Utilization rate (percentage of hours blocked)
    CASE WHEN total_weekly_net_available_hours > 0 
        THEN ROUND((monday_busy_hours + tuesday_busy_hours + wednesday_busy_hours + thursday_busy_hours + friday_busy_hours + saturday_busy_hours + sunday_busy_hours) / total_weekly_net_available_hours * 100, 1)
        ELSE 0 
    END AS busy_hours_percentage,
    
    -- Weekly actual availability count
    (monday_actual_availability + tuesday_actual_availability + wednesday_actual_availability + 
     thursday_actual_availability + friday_actual_availability + saturday_actual_availability + 
     sunday_actual_availability) AS total_days_with_availability,
     
    -- Weekday and weekend actual availability counts
    (monday_actual_availability + tuesday_actual_availability + wednesday_actual_availability + 
     thursday_actual_availability + friday_actual_availability) AS weekdays_with_availability,
     
    (saturday_actual_availability + sunday_actual_availability) AS weekend_days_with_availability,
    
    -- Daily segments (for available hours)
    ROUND(monday_morning_available_hours, 2) AS monday_morning_net_hours,
    ROUND(monday_afternoon_available_hours, 2) AS monday_afternoon_net_hours,
    ROUND(monday_evening_available_hours, 2) AS monday_evening_net_hours,
    ROUND(tuesday_morning_available_hours, 2) AS tuesday_morning_net_hours,
    ROUND(tuesday_afternoon_available_hours, 2) AS tuesday_afternoon_net_hours,
    ROUND(tuesday_evening_available_hours, 2) AS tuesday_evening_net_hours,
    ROUND(wednesday_morning_available_hours, 2) AS wednesday_morning_net_hours,
    ROUND(wednesday_afternoon_available_hours, 2) AS wednesday_afternoon_net_hours,
    ROUND(wednesday_evening_available_hours, 2) AS wednesday_evening_net_hours,
    ROUND(thursday_morning_available_hours, 2) AS thursday_morning_net_hours,
    ROUND(thursday_afternoon_available_hours, 2) AS thursday_afternoon_net_hours,
    ROUND(thursday_evening_available_hours, 2) AS thursday_evening_net_hours,
    ROUND(friday_morning_available_hours, 2) AS friday_morning_net_hours,
    ROUND(friday_afternoon_available_hours, 2) AS friday_afternoon_net_hours,
    ROUND(friday_evening_available_hours, 2) AS friday_evening_net_hours,
    ROUND(saturday_morning_available_hours, 2) AS saturday_morning_net_hours,
    ROUND(saturday_afternoon_available_hours, 2) AS saturday_afternoon_net_hours,
    ROUND(saturday_evening_available_hours, 2) AS saturday_evening_net_hours,
    ROUND(sunday_morning_available_hours, 2) AS sunday_morning_net_hours,
    ROUND(sunday_afternoon_available_hours, 2) AS sunday_afternoon_net_hours,
    ROUND(sunday_evening_available_hours, 2) AS sunday_evening_net_hours,
    
    -- Weekly segments (sum of daily segments)
    ROUND((monday_morning_available_hours + tuesday_morning_available_hours + wednesday_morning_available_hours + 
     thursday_morning_available_hours + friday_morning_available_hours + saturday_morning_available_hours + 
     sunday_morning_available_hours), 2) AS weekly_morning_net_hours,
    
    ROUND((monday_afternoon_available_hours + tuesday_afternoon_available_hours + wednesday_afternoon_available_hours + 
     thursday_afternoon_available_hours + friday_afternoon_available_hours + saturday_afternoon_available_hours + 
     sunday_afternoon_available_hours), 2) AS weekly_afternoon_net_hours,
    
    ROUND((monday_evening_available_hours + tuesday_evening_available_hours + wednesday_evening_available_hours + 
     thursday_evening_available_hours + friday_evening_available_hours + saturday_evening_available_hours + 
     sunday_evening_available_hours), 2) AS weekly_evening_net_hours,
    
    -- Available weekdays and weekend days
    (CASE WHEN monday_net_available_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN tuesday_net_available_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN wednesday_net_available_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN thursday_net_available_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN friday_net_available_hours > 0 THEN 1 ELSE 0 END) AS count_of_weekdays_with_hours,
     
    (CASE WHEN saturday_net_available_hours > 0 THEN 1 ELSE 0 END +
     CASE WHEN sunday_net_available_hours > 0 THEN 1 ELSE 0 END) AS count_of_weekend_days_with_hours
FROM weekly_availability
WHERE week_start_date >= DATE_TRUNC('week', CURRENT_DATE())
  AND week_start_date <= DATEADD('week', 4, DATE_TRUNC('week', CURRENT_DATE()))
  AND coral_provider_id IS NOT NULL 
ORDER BY 
    provider_detail,
    week_start_date