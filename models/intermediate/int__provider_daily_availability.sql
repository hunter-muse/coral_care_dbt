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
        
        -- Calculate total available hours directly from time_slot with rounding to 2 decimal places
        CASE 
            WHEN d.day_of_week = 'Mon' AND p.monday_availability IS NOT NULL THEN
                ROUND(DATEDIFF('minute',
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM')
                ) / 60.0, 2)
            WHEN d.day_of_week = 'Tue' AND p.tuesday_availability IS NOT NULL THEN
                ROUND(DATEDIFF('minute',
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM')
                ) / 60.0, 2)
            WHEN d.day_of_week = 'Wed' AND p.wednesday_availability IS NOT NULL THEN
                ROUND(DATEDIFF('minute',
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.wednesday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.wednesday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM')
                ) / 60.0, 2)
            WHEN d.day_of_week = 'Thu' AND p.thursday_availability IS NOT NULL THEN
                ROUND(DATEDIFF('minute',
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.thursday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.thursday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM')
                ) / 60.0, 2)
            WHEN d.day_of_week = 'Fri' AND p.friday_availability IS NOT NULL THEN
                ROUND(DATEDIFF('minute',
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.friday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.friday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM')
                ) / 60.0, 2)
            WHEN d.day_of_week = 'Sat' AND p.saturday_availability IS NOT NULL THEN
                ROUND(DATEDIFF('minute',
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.saturday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.saturday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM')
                ) / 60.0, 2)
            WHEN d.day_of_week = 'Sun' AND p.sunday_availability IS NOT NULL THEN
                ROUND(DATEDIFF('minute',
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.sunday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.sunday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM')
                ) / 60.0, 2)
            ELSE 
                CASE 
                    WHEN d.day_of_week = 'Mon' THEN p.monday_total_hours
                    WHEN d.day_of_week = 'Tue' THEN p.tuesday_total_hours
                    WHEN d.day_of_week = 'Wed' THEN p.wednesday_total_hours
                    WHEN d.day_of_week = 'Thu' THEN p.thursday_total_hours
                    WHEN d.day_of_week = 'Fri' THEN p.friday_total_hours
                    WHEN d.day_of_week = 'Sat' THEN p.saturday_total_hours
                    WHEN d.day_of_week = 'Sun' THEN p.sunday_total_hours
                    ELSE 0
                END
        END AS total_available_hours,
        
        -- Morning hours by day with rounding to 2 decimal places
        CASE 
            WHEN d.day_of_week = 'Mon' THEN 
                CASE 
                    WHEN p.monday_availability IS NOT NULL THEN
                        CASE WHEN 
                            GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 05:00:00'), 
                                     TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')) <
                            LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 12:00:00'), 
                                  TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                        THEN
                            ROUND(DATEDIFF('minute', 
                                GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 05:00:00'), 
                                         TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')),
                                LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 12:00:00'), 
                                      TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                            ) / 60.0, 2)
                        ELSE 0
                        END
                    ELSE p.monday_morning_hours
                END
            WHEN d.day_of_week = 'Tue' THEN 
                CASE 
                    WHEN p.tuesday_availability IS NOT NULL THEN
                        CASE WHEN 
                            GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 05:00:00'), 
                                     TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')) <
                            LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 12:00:00'), 
                                  TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                        THEN
                            ROUND(DATEDIFF('minute', 
                                GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 05:00:00'), 
                                         TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')),
                                LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 12:00:00'), 
                                      TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                            ) / 60.0, 2)
                        ELSE 0
                        END
                    ELSE p.tuesday_morning_hours
                END
            WHEN d.day_of_week = 'Wed' THEN p.wednesday_morning_hours
            WHEN d.day_of_week = 'Thu' THEN p.thursday_morning_hours
            WHEN d.day_of_week = 'Fri' THEN p.friday_morning_hours
            WHEN d.day_of_week = 'Sat' THEN p.saturday_morning_hours
            WHEN d.day_of_week = 'Sun' THEN p.sunday_morning_hours
            ELSE 0
        END AS morning_hours,
        
        -- Afternoon hours by day with rounding to 2 decimal places
        CASE 
            WHEN d.day_of_week = 'Mon' THEN 
                CASE 
                    WHEN p.monday_availability IS NOT NULL THEN
                        CASE WHEN 
                            GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 12:00:00'), 
                                     TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')) <
                            LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 17:00:00'), 
                                  TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                        THEN
                            ROUND(DATEDIFF('minute', 
                                GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 12:00:00'), 
                                         TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')),
                                LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 17:00:00'), 
                                      TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                            ) / 60.0, 2)
                        ELSE 0
                        END
                    ELSE p.monday_afternoon_hours
                END
            WHEN d.day_of_week = 'Tue' THEN 
                CASE 
                    WHEN p.tuesday_availability IS NOT NULL THEN
                        CASE WHEN 
                            GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 12:00:00'), 
                                     TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')) <
                            LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 17:00:00'), 
                                  TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                        THEN
                            ROUND(DATEDIFF('minute', 
                                GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 12:00:00'), 
                                         TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')),
                                LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 17:00:00'), 
                                      TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                            ) / 60.0, 2)
                        ELSE 0
                        END
                    ELSE p.tuesday_afternoon_hours
                END
            WHEN d.day_of_week = 'Wed' THEN p.wednesday_afternoon_hours
            WHEN d.day_of_week = 'Thu' THEN p.thursday_afternoon_hours
            WHEN d.day_of_week = 'Fri' THEN p.friday_afternoon_hours
            WHEN d.day_of_week = 'Sat' THEN p.saturday_afternoon_hours
            WHEN d.day_of_week = 'Sun' THEN p.sunday_afternoon_hours
            ELSE 0
        END AS afternoon_hours,
        
        -- Evening hours by day with rounding to 2 decimal places
        CASE 
            WHEN d.day_of_week = 'Mon' THEN 
                CASE 
                    WHEN p.monday_availability IS NOT NULL THEN
                        CASE WHEN 
                            GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 17:00:00'), 
                                     TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')) <
                            LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 22:00:00'), 
                                  TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                        THEN
                            ROUND(DATEDIFF('minute', 
                                GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 17:00:00'), 
                                         TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')),
                                LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 22:00:00'), 
                                      TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.monday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                            ) / 60.0, 2)
                        ELSE 0
                        END
                    ELSE p.monday_evening_hours
                END
            WHEN d.day_of_week = 'Tue' THEN 
                CASE 
                    WHEN p.tuesday_availability IS NOT NULL THEN
                        CASE WHEN 
                            GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 17:00:00'), 
                                     TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')) <
                            LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 22:00:00'), 
                                  TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                        THEN
                            ROUND(DATEDIFF('minute', 
                                GREATEST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 17:00:00'), 
                                         TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 1)), 'YYYY-MM-DD HH:MI AM')),
                                LEAST(TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' 22:00:00'), 
                                      TRY_TO_TIMESTAMP(TO_CHAR(CURRENT_DATE()) || ' ' || TRIM(SPLIT_PART(p.tuesday_availability, '-', 2)), 'YYYY-MM-DD HH:MI AM'))
                            ) / 60.0, 2)
                        ELSE 0
                        END
                    ELSE p.tuesday_evening_hours
                END
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

busy_hours_by_day AS (
    SELECT
        provider_detail,
        DATE(start_date) AS busy_date,
        -- Total busy hours with rounding to 2 decimal places
        ROUND(SUM(DATEDIFF('hour', start_date, end_date)), 2) AS total_busy_hours,
        
        -- Morning segment with rounding to 2 decimal places
        ROUND(SUM(CASE 
            WHEN HOUR(end_date) <= 5 OR HOUR(start_date) >= 12 THEN 0
            ELSE DATEDIFF('minute', 
                GREATEST(start_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 05:00:00')), 
                LEAST(end_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 12:00:00'))
            ) / 60.0
        END), 2) AS morning_busy_hours,
        
        -- Afternoon segment with rounding to 2 decimal places
        ROUND(SUM(CASE 
            WHEN HOUR(end_date) <= 12 OR HOUR(start_date) >= 17 THEN 0
            ELSE DATEDIFF('minute', 
                GREATEST(start_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 12:00:00')), 
                LEAST(end_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 17:00:00'))
            ) / 60.0
        END), 2) AS afternoon_busy_hours,
        
        -- Evening segment with rounding to 2 decimal places
        ROUND(SUM(CASE 
            WHEN HOUR(end_date) <= 17 OR HOUR(start_date) >= 22 THEN 0
            ELSE DATEDIFF('minute', 
                GREATEST(start_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 17:00:00')), 
                LEAST(end_date, TO_TIMESTAMP(TO_CHAR(DATE(start_date)) || ' 22:00:00'))
            ) / 60.0
        END), 2) AS evening_busy_hours
    FROM {{ref('stg__bubble__busyslot')}}
    WHERE 
        delete_flag = 'false'
        AND DATE(start_date) >= DATE_TRUNC('week', CURRENT_DATE())
        AND DATE(start_date) <= DATEADD('day', 28, CURRENT_DATE())
    GROUP BY 
        provider_detail,
        busy_date
),

overlapping_busy_hours AS (
    -- Calculate busy hours that overlap with provider's templated availability
    SELECT
        pd.provider_detail,
        pd.calendar_date,
        
        -- Morning segment overlap with rounding to 2 decimal places
        ROUND(SUM(
            CASE WHEN 
                pd.time_slot IS NOT NULL AND
                GREATEST(
                    TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    bs.start_date,
                    TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 05:00:00')
                ) < 
                LEAST(
                    TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 2)), 'YYYY-MM-DD HH:MI AM'),
                    bs.end_date,
                    TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 12:00:00')
                )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                        bs.start_date,
                        TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 05:00:00')
                    ),
                    LEAST(
                        TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 2)), 'YYYY-MM-DD HH:MI AM'),
                        bs.end_date,
                        TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 12:00:00')
                    )
                ) / 60.0, 2)
            ELSE 0
            END
        ), 2) AS morning_overlapping_busy_hours,
        
        -- Afternoon segment overlap with rounding to 2 decimal places
        ROUND(SUM(
            CASE WHEN 
                pd.time_slot IS NOT NULL AND
                GREATEST(
                    TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    bs.start_date,
                    TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 12:00:00')
                ) < 
                LEAST(
                    TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 2)), 'YYYY-MM-DD HH:MI AM'),
                    bs.end_date,
                    TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 17:00:00')
                )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                        bs.start_date,
                        TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 12:00:00')
                    ),
                    LEAST(
                        TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 2)), 'YYYY-MM-DD HH:MI AM'),
                        bs.end_date,
                        TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 17:00:00')
                    )
                ) / 60.0, 2)
            ELSE 0
            END
        ), 2) AS afternoon_overlapping_busy_hours,
        
        -- Evening segment overlap with rounding to 2 decimal places
        ROUND(SUM(
            CASE WHEN 
                pd.time_slot IS NOT NULL AND
                GREATEST(
                    TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    bs.start_date,
                    TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 17:00:00')
                ) < 
                LEAST(
                    TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 2)), 'YYYY-MM-DD HH:MI AM'),
                    bs.end_date,
                    TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 22:00:00')
                )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                        bs.start_date,
                        TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 17:00:00')
                    ),
                    LEAST(
                        TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 2)), 'YYYY-MM-DD HH:MI AM'),
                        bs.end_date,
                        TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' 22:00:00')
                    )
                ) / 60.0, 2)
            ELSE 0
            END
        ), 2) AS evening_overlapping_busy_hours,
        
        -- Total overlapping busy hours with rounding to 2 decimal places
        ROUND(SUM(
            CASE WHEN 
                pd.time_slot IS NOT NULL AND
                GREATEST(
                    TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                    bs.start_date
                ) < 
                LEAST(
                    TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 2)), 'YYYY-MM-DD HH:MI AM'),
                    bs.end_date
                )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 1)), 'YYYY-MM-DD HH:MI AM'),
                        bs.start_date
                    ),
                    LEAST(
                        TRY_TO_TIMESTAMP(TO_CHAR(pd.calendar_date) || ' ' || TRIM(SPLIT_PART(pd.time_slot, '-', 2)), 'YYYY-MM-DD HH:MI AM'),
                        bs.end_date
                    )
                ) / 60.0, 2)
            ELSE 0
            END
        ), 2) AS total_overlapping_busy_hours
    FROM provider_days pd
    JOIN {{ref('stg__bubble__busyslot')}} bs ON 
        pd.provider_detail = bs.provider_detail 
        AND pd.calendar_date = DATE(bs.start_date)
    WHERE 
        bs.delete_flag = 'false'
    GROUP BY 
        pd.provider_detail,
        pd.calendar_date
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
    COALESCE(bh.total_busy_hours, 0) AS total_busy_hours,
    pd.total_available_hours,
    
    -- Net available hours (available minus overlapping busy hours)
    GREATEST(pd.total_available_hours - COALESCE(obh.total_overlapping_busy_hours, 0), 0) AS net_available_hours,
    
    -- Original segment hours (from template)
    pd.morning_hours,
    pd.afternoon_hours,
    pd.evening_hours,
    
    -- Total busy hours by segment (regardless of template)
    COALESCE(bh.morning_busy_hours, 0) AS morning_busy_hours,
    COALESCE(bh.afternoon_busy_hours, 0) AS afternoon_busy_hours,
    COALESCE(bh.evening_busy_hours, 0) AS evening_busy_hours,
    
    -- Overlapping busy hours by segment (only within template)
    COALESCE(obh.morning_overlapping_busy_hours, 0) AS morning_overlapping_busy_hours,
    COALESCE(obh.afternoon_overlapping_busy_hours, 0) AS afternoon_overlapping_busy_hours,
    COALESCE(obh.evening_overlapping_busy_hours, 0) AS evening_overlapping_busy_hours,
    
    -- Net available hours by segment (available minus overlapping busy hours)
    GREATEST(pd.morning_hours - COALESCE(obh.morning_overlapping_busy_hours, 0), 0) AS net_morning_hours,
    GREATEST(pd.afternoon_hours - COALESCE(obh.afternoon_overlapping_busy_hours, 0), 0) AS net_afternoon_hours,
    GREATEST(pd.evening_hours - COALESCE(obh.evening_overlapping_busy_hours, 0), 0) AS net_evening_hours
FROM provider_days pd
LEFT JOIN busy_hours_by_day bh ON 
    pd.provider_detail = bh.provider_detail 
    AND pd.calendar_date = bh.busy_date
LEFT JOIN overlapping_busy_hours obh ON
    pd.provider_detail = obh.provider_detail
    AND pd.calendar_date = obh.calendar_date
ORDER BY 
    pd.provider_detail, 
    pd.calendar_date



