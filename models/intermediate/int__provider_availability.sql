WITH provider_availability AS (
  SELECT 
    provider_detail,
    provider.coral_provider_id,
    working_days,
    start_time,
    end_time,
    TO_CHAR(start_time, 'HH12:MI AM')||'-'||TO_CHAR(end_time, 'HH12:MI AM') AS time_slot,
    -- Use minutes and divide by 60.0 to get decimal hours, then round to 2 decimal places
    ROUND(DATEDIFF('minute', start_time, end_time) / 60.0, 2) AS total_hours,
    
    -- Morning hours (8:00 AM to 11:59 AM) with decimal precision
    CASE 
      WHEN HOUR(end_time) <= 8 THEN 0
      WHEN HOUR(start_time) >= 12 THEN 0
      ELSE 
        ROUND(DATEDIFF('minute', 
          GREATEST(start_time, DATEADD('hour', 8, DATE_TRUNC('day', start_time))),
          LEAST(end_time, DATEADD('hour', 12, DATE_TRUNC('day', start_time)))
        ) / 60.0, 2)
    END AS morning_hours,
    
    -- Afternoon hours (12:00 PM to 4:59 PM) with decimal precision
    CASE 
      WHEN HOUR(end_time) <= 12 THEN 0
      WHEN HOUR(start_time) >= 17 THEN 0
      ELSE 
        ROUND(DATEDIFF('minute', 
          GREATEST(start_time, DATEADD('hour', 12, DATE_TRUNC('day', start_time))),
          LEAST(end_time, DATEADD('hour', 17, DATE_TRUNC('day', start_time)))
        ) / 60.0, 2)
    END AS afternoon_hours,
    
    -- Evening hours (5:00 PM and later) with decimal precision
    CASE 
      WHEN HOUR(end_time) <= 17 THEN 0
      ELSE 
        ROUND(DATEDIFF('minute', 
          GREATEST(start_time, DATEADD('hour', 17, DATE_TRUNC('day', start_time))),
          end_time
        ) / 60.0, 2)
    END AS evening_hours
  FROM {{ref('stg__bubble__availability')}} availability
  left join {{ref('int__provider')}} provider on provider_detail = provider.bubble_provider_id
)

SELECT 
  provider_detail,
  coral_provider_id,
  -- Daily availability with comma-separated time slots
  LISTAGG(CASE WHEN CONTAINS(working_days, 'Monday') THEN time_slot END, ', ') WITHIN GROUP (ORDER BY start_time) AS monday_availability,
  LISTAGG(CASE WHEN CONTAINS(working_days, 'Tuesday') THEN time_slot END, ', ') WITHIN GROUP (ORDER BY start_time) AS tuesday_availability,
  LISTAGG(CASE WHEN CONTAINS(working_days, 'Wednesday') THEN time_slot END, ', ') WITHIN GROUP (ORDER BY start_time) AS wednesday_availability,
  LISTAGG(CASE WHEN CONTAINS(working_days, 'Thursday') THEN time_slot END, ', ') WITHIN GROUP (ORDER BY start_time) AS thursday_availability,
  LISTAGG(CASE WHEN CONTAINS(working_days, 'Friday') THEN time_slot END, ', ') WITHIN GROUP (ORDER BY start_time) AS friday_availability,
  LISTAGG(CASE WHEN CONTAINS(working_days, 'Saturday') THEN time_slot END, ', ') WITHIN GROUP (ORDER BY start_time) AS saturday_availability,
  LISTAGG(CASE WHEN CONTAINS(working_days, 'Sunday') THEN time_slot END, ', ') WITHIN GROUP (ORDER BY start_time) AS sunday_availability,
  
  -- Daily total hours
  SUM(CASE WHEN CONTAINS(working_days, 'Monday') THEN total_hours ELSE 0 END) AS monday_total_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Tuesday') THEN total_hours ELSE 0 END) AS tuesday_total_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Wednesday') THEN total_hours ELSE 0 END) AS wednesday_total_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Thursday') THEN total_hours ELSE 0 END) AS thursday_total_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Friday') THEN total_hours ELSE 0 END) AS friday_total_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Saturday') THEN total_hours ELSE 0 END) AS saturday_total_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Sunday') THEN total_hours ELSE 0 END) AS sunday_total_hours,
  
  -- Weekly total hours
  SUM(total_hours) AS weekly_total_hours,
  
  -- Daily segment hours
  SUM(CASE WHEN CONTAINS(working_days, 'Monday') THEN morning_hours ELSE 0 END) AS monday_morning_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Monday') THEN afternoon_hours ELSE 0 END) AS monday_afternoon_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Monday') THEN evening_hours ELSE 0 END) AS monday_evening_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Tuesday') THEN morning_hours ELSE 0 END) AS tuesday_morning_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Tuesday') THEN afternoon_hours ELSE 0 END) AS tuesday_afternoon_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Tuesday') THEN evening_hours ELSE 0 END) AS tuesday_evening_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Wednesday') THEN morning_hours ELSE 0 END) AS wednesday_morning_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Wednesday') THEN afternoon_hours ELSE 0 END) AS wednesday_afternoon_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Wednesday') THEN evening_hours ELSE 0 END) AS wednesday_evening_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Thursday') THEN morning_hours ELSE 0 END) AS Thursday_morning_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Thursday') THEN afternoon_hours ELSE 0 END) AS Thursday_afternoon_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Thursday') THEN evening_hours ELSE 0 END) AS Thursday_evening_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Friday') THEN morning_hours ELSE 0 END) AS Friday_morning_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Friday') THEN afternoon_hours ELSE 0 END) AS Friday_afternoon_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Friday') THEN evening_hours ELSE 0 END) AS Friday_evening_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Saturday') THEN morning_hours ELSE 0 END) AS Saturday_morning_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Saturday') THEN afternoon_hours ELSE 0 END) AS Saturday_afternoon_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Saturday') THEN evening_hours ELSE 0 END) AS Saturday_evening_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Sunday') THEN morning_hours ELSE 0 END) AS Sunday_morning_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Sunday') THEN afternoon_hours ELSE 0 END) AS Sunday_afternoon_hours,
  SUM(CASE WHEN CONTAINS(working_days, 'Sunday') THEN evening_hours ELSE 0 END) AS Sunday_evening_hours,
  
  -- Weekly segment hours
  SUM(morning_hours) AS weekly_morning_hours,
  SUM(afternoon_hours) AS weekly_afternoon_hours,
  SUM(evening_hours) AS weekly_evening_hours

FROM provider_availability
GROUP BY provider_detail, coral_provider_id


