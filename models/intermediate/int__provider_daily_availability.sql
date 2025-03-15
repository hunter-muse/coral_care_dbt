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

provider_time_slots AS (
    -- Join provider availability with date spine and prepare for slot expansion
    SELECT 
        p.provider_detail,
        p.coral_provider_id,
        d.calendar_date,
        d.day_of_week,
        d.time_period,
        d.week_number,
        
        -- Get the appropriate day's availability
        CASE 
            WHEN d.day_of_week = 'Mon' THEN p.monday_availability
            WHEN d.day_of_week = 'Tue' THEN p.tuesday_availability
            WHEN d.day_of_week = 'Wed' THEN p.wednesday_availability
            WHEN d.day_of_week = 'Thu' THEN p.thursday_availability
            WHEN d.day_of_week = 'Fri' THEN p.friday_availability
            WHEN d.day_of_week = 'Sat' THEN p.saturday_availability
            WHEN d.day_of_week = 'Sun' THEN p.sunday_availability
            ELSE NULL
        END AS day_availability,
        
        -- Store the original hours for reference
        CASE 
            WHEN d.day_of_week = 'Mon' THEN p.monday_total_hours
            WHEN d.day_of_week = 'Tue' THEN p.tuesday_total_hours
            WHEN d.day_of_week = 'Wed' THEN p.wednesday_total_hours
            WHEN d.day_of_week = 'Thu' THEN p.thursday_total_hours
            WHEN d.day_of_week = 'Fri' THEN p.friday_total_hours
            WHEN d.day_of_week = 'Sat' THEN p.saturday_total_hours
            WHEN d.day_of_week = 'Sun' THEN p.sunday_total_hours
            ELSE 0
        END AS original_total_hours,
        
        -- Store the original segment hours with unique names for each day
        CASE WHEN d.day_of_week = 'Mon' THEN p.monday_morning_hours ELSE 0 END AS mon_morning_hours,
        CASE WHEN d.day_of_week = 'Mon' THEN p.monday_afternoon_hours ELSE 0 END AS mon_afternoon_hours,
        CASE WHEN d.day_of_week = 'Mon' THEN p.monday_evening_hours ELSE 0 END AS mon_evening_hours,
        CASE WHEN d.day_of_week = 'Tue' THEN p.tuesday_morning_hours ELSE 0 END AS tue_morning_hours,
        CASE WHEN d.day_of_week = 'Tue' THEN p.tuesday_afternoon_hours ELSE 0 END AS tue_afternoon_hours,
        CASE WHEN d.day_of_week = 'Tue' THEN p.tuesday_evening_hours ELSE 0 END AS tue_evening_hours,
        CASE WHEN d.day_of_week = 'Wed' THEN p.wednesday_morning_hours ELSE 0 END AS wed_morning_hours,
        CASE WHEN d.day_of_week = 'Wed' THEN p.wednesday_afternoon_hours ELSE 0 END AS wed_afternoon_hours,
        CASE WHEN d.day_of_week = 'Wed' THEN p.wednesday_evening_hours ELSE 0 END AS wed_evening_hours,
        CASE WHEN d.day_of_week = 'Thu' THEN p.thursday_morning_hours ELSE 0 END AS thu_morning_hours,
        CASE WHEN d.day_of_week = 'Thu' THEN p.thursday_afternoon_hours ELSE 0 END AS thu_afternoon_hours,
        CASE WHEN d.day_of_week = 'Thu' THEN p.thursday_evening_hours ELSE 0 END AS thu_evening_hours,
        CASE WHEN d.day_of_week = 'Fri' THEN p.friday_morning_hours ELSE 0 END AS fri_morning_hours,
        CASE WHEN d.day_of_week = 'Fri' THEN p.friday_afternoon_hours ELSE 0 END AS fri_afternoon_hours,
        CASE WHEN d.day_of_week = 'Fri' THEN p.friday_evening_hours ELSE 0 END AS fri_evening_hours,
        CASE WHEN d.day_of_week = 'Sat' THEN p.saturday_morning_hours ELSE 0 END AS sat_morning_hours,
        CASE WHEN d.day_of_week = 'Sat' THEN p.saturday_afternoon_hours ELSE 0 END AS sat_afternoon_hours,
        CASE WHEN d.day_of_week = 'Sat' THEN p.saturday_evening_hours ELSE 0 END AS sat_evening_hours,
        CASE WHEN d.day_of_week = 'Sun' THEN p.sunday_morning_hours ELSE 0 END AS sun_morning_hours,
        CASE WHEN d.day_of_week = 'Sun' THEN p.sunday_afternoon_hours ELSE 0 END AS sun_afternoon_hours,
        CASE WHEN d.day_of_week = 'Sun' THEN p.sunday_evening_hours ELSE 0 END AS sun_evening_hours
    FROM provider_base_availability p
    CROSS JOIN date_spine d
),

expanded_time_slots AS (
    -- Split multiple time slots into separate rows
    SELECT
        provider_detail,
        coral_provider_id,
        calendar_date,
        day_of_week,
        time_period,
        week_number,
        mon_morning_hours,
        mon_afternoon_hours,
        mon_evening_hours,
        tue_morning_hours,
        tue_afternoon_hours,
        tue_evening_hours,
        wed_morning_hours,
        wed_afternoon_hours,
        wed_evening_hours,
        thu_morning_hours,
        thu_afternoon_hours,
        thu_evening_hours,
        fri_morning_hours,
        fri_afternoon_hours,
        fri_evening_hours,
        sat_morning_hours,
        sat_afternoon_hours,
        sat_evening_hours,
        sun_morning_hours,
        sun_afternoon_hours,
        sun_evening_hours,
        TRIM(value) AS time_slot,
        TRIM(SPLIT_PART(TRIM(value), '-', 1)) AS start_time,
        TRIM(SPLIT_PART(TRIM(value), '-', 2)) AS end_time
    FROM provider_time_slots,
    LATERAL FLATTEN(input => SPLIT(day_availability, ','))
    WHERE day_availability IS NOT NULL
),

calculated_slot_hours AS (
    -- Calculate hours for each individual time slot
    SELECT
        provider_detail,
        coral_provider_id,
        calendar_date,
        day_of_week,
        time_period,
        week_number,
        time_slot,
        start_time,
        end_time,
        
        -- Calculate total hours for this slot with better format handling
        CASE 
            WHEN start_time IS NULL OR end_time IS NULL OR start_time = '' OR end_time = '' THEN 0
            ELSE ROUND(DATEDIFF('minute',
                TRY_TO_TIME(start_time),
                TRY_TO_TIME(end_time)
            ) / 60.0, 2)
        END AS total_available_hours,
        
        -- Calculate morning hours (5am-12pm) with better format handling
        CASE 
            WHEN start_time IS NULL OR end_time IS NULL OR start_time = '' OR end_time = '' THEN 0
            WHEN GREATEST(
                TRY_TO_TIME(start_time),
                TRY_TO_TIME('05:00 AM')
            ) < 
            LEAST(
                TRY_TO_TIME(end_time),
                TRY_TO_TIME('12:00 PM')
            )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIME(start_time),
                        TRY_TO_TIME('05:00 AM')
                    ),
                    LEAST(
                        TRY_TO_TIME(end_time),
                        TRY_TO_TIME('12:00 PM')
                    )
                ) / 60.0, 2)
            ELSE 0
        END AS morning_hours,
        
        -- Calculate afternoon hours (12pm-5pm) with better format handling
        CASE 
            WHEN start_time IS NULL OR end_time IS NULL OR start_time = '' OR end_time = '' THEN 0
            WHEN GREATEST(
                TRY_TO_TIME(start_time),
                TRY_TO_TIME('12:00 PM')
            ) < 
            LEAST(
                TRY_TO_TIME(end_time),
                TRY_TO_TIME('05:00 PM')
            )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIME(start_time),
                        TRY_TO_TIME('12:00 PM')
                    ),
                    LEAST(
                        TRY_TO_TIME(end_time),
                        TRY_TO_TIME('05:00 PM')
                    )
                ) / 60.0, 2)
            ELSE 0
        END AS afternoon_hours,
        
        -- Calculate evening hours (5pm-10pm) with better format handling
        CASE 
            WHEN start_time IS NULL OR end_time IS NULL OR start_time = '' OR end_time = '' THEN 0
            WHEN GREATEST(
                TRY_TO_TIME(start_time),
                TRY_TO_TIME('05:00 PM')
            ) < 
            LEAST(
                TRY_TO_TIME(end_time),
                TRY_TO_TIME('10:00 PM')
            )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIME(start_time),
                        TRY_TO_TIME('05:00 PM')
                    ),
                    LEAST(
                        TRY_TO_TIME(end_time),
                        TRY_TO_TIME('10:00 PM')
                    )
                ) / 60.0, 2)
            ELSE 0
        END AS evening_hours
    FROM expanded_time_slots
),

busy_hours_by_day AS (
    SELECT
        provider_detail,
        DATE(start_date) AS busy_date,
        -- Total busy hours with rounding to 2 decimal places
        ROUND(SUM(DATEDIFF('minute', start_date, end_date) / 60.0), 2) AS total_busy_hours,
        
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
    -- Calculate busy hours that overlap with each time slot
    SELECT
        ets.provider_detail,
        ets.calendar_date,
        ets.time_slot,
        
        -- Morning segment overlap with rounding to 2 decimal places
        ROUND(SUM(
            CASE WHEN 
                GREATEST(
                    TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.start_time, 'YYYY-MM-DD HH:MI AM'),
                    bs.start_date,
                    TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 05:00:00')
                ) < 
                LEAST(
                    TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.end_time, 'YYYY-MM-DD HH:MI AM'),
                    bs.end_date,
                    TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 12:00:00')
                )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.start_time, 'YYYY-MM-DD HH:MI AM'),
                        bs.start_date,
                        TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 05:00:00')
                    ),
                    LEAST(
                        TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.end_time, 'YYYY-MM-DD HH:MI AM'),
                        bs.end_date,
                        TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 12:00:00')
                    )
                ) / 60.0, 2)
            ELSE 0
            END
        ), 2) AS morning_overlapping_busy_hours,
        
        -- Afternoon segment overlap with rounding to 2 decimal places
        ROUND(SUM(
            CASE WHEN 
                GREATEST(
                    TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.start_time, 'YYYY-MM-DD HH:MI AM'),
                    bs.start_date,
                    TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 12:00:00')
                ) < 
                LEAST(
                    TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.end_time, 'YYYY-MM-DD HH:MI AM'),
                    bs.end_date,
                    TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 17:00:00')
                )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.start_time, 'YYYY-MM-DD HH:MI AM'),
                        bs.start_date,
                        TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 12:00:00')
                    ),
                    LEAST(
                        TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.end_time, 'YYYY-MM-DD HH:MI AM'),
                        bs.end_date,
                        TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 17:00:00')
                    )
                ) / 60.0, 2)
            ELSE 0
            END
        ), 2) AS afternoon_overlapping_busy_hours,
        
        -- Evening segment overlap with rounding to 2 decimal places
        ROUND(SUM(
            CASE WHEN 
                GREATEST(
                    TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.start_time, 'YYYY-MM-DD HH:MI AM'),
                    bs.start_date,
                    TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 17:00:00')
                ) < 
                LEAST(
                    TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.end_time, 'YYYY-MM-DD HH:MI AM'),
                    bs.end_date,
                    TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 22:00:00')
                )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.start_time, 'YYYY-MM-DD HH:MI AM'),
                        bs.start_date,
                        TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 17:00:00')
                    ),
                    LEAST(
                        TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.end_time, 'YYYY-MM-DD HH:MI AM'),
                        bs.end_date,
                        TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' 22:00:00')
                    )
                ) / 60.0, 2)
            ELSE 0
            END
        ), 2) AS evening_overlapping_busy_hours,
        
        -- Total overlapping busy hours with rounding to 2 decimal places
        ROUND(SUM(
            CASE WHEN 
                GREATEST(
                    TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.start_time, 'YYYY-MM-DD HH:MI AM'),
                    bs.start_date
                ) < 
                LEAST(
                    TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.end_time, 'YYYY-MM-DD HH:MI AM'),
                    bs.end_date
                )
            THEN
                ROUND(DATEDIFF('minute',
                    GREATEST(
                        TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.start_time, 'YYYY-MM-DD HH:MI AM'),
                        bs.start_date
                    ),
                    LEAST(
                        TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.end_time, 'YYYY-MM-DD HH:MI AM'),
                        bs.end_date
                    )
                ) / 60.0, 2)
            ELSE 0
            END
        ), 2) AS total_overlapping_busy_hours
    FROM expanded_time_slots ets
    JOIN {{ref('stg__bubble__busyslot')}} bs ON 
        ets.provider_detail = bs.provider_detail 
        AND ets.calendar_date = DATE(bs.start_date)
    WHERE 
        bs.delete_flag = 'false'
    GROUP BY 
        ets.provider_detail,
        ets.calendar_date,
        ets.time_slot
),

busy_slots AS (
    -- Extract individual busy slots for each provider and date
    SELECT
        provider_detail,
        DATE(start_date) AS busy_date,
        start_date AS busy_start,
        end_date AS busy_end
    FROM {{ref('stg__bubble__busyslot')}}
    WHERE 
        delete_flag = 'false'
        AND DATE(start_date) >= DATE_TRUNC('week', CURRENT_DATE())
        AND DATE(start_date) <= DATEADD('day', 28, CURRENT_DATE())
),

availability_with_busy_slots AS (
    -- Join expanded time slots with busy slots to prepare for calculating actual availability
    SELECT
        ets.provider_detail,
        ets.coral_provider_id,
        ets.calendar_date,
        ets.time_slot,
        TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.start_time, 'YYYY-MM-DD HH:MI AM') AS slot_start,
        TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.end_time, 'YYYY-MM-DD HH:MI AM') AS slot_end,
        bs.busy_start,
        bs.busy_end
    FROM expanded_time_slots ets
    LEFT JOIN busy_slots bs ON 
        ets.provider_detail = bs.provider_detail 
        AND ets.calendar_date = bs.busy_date
        AND bs.busy_start < TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.end_time, 'YYYY-MM-DD HH:MI AM')
        AND bs.busy_end > TRY_TO_TIMESTAMP(TO_CHAR(ets.calendar_date) || ' ' || ets.start_time, 'YYYY-MM-DD HH:MI AM')
),

actual_availability_segments AS (
    -- Calculate actual availability segments by finding gaps between busy slots
    SELECT
        provider_detail,
        coral_provider_id,
        calendar_date,
        time_slot,
        slot_start,
        slot_end,
        -- Use LISTAGG to collect all actual available time segments with 12-hour format
        LISTAGG(
            TO_CHAR(segment_start, 'HH12:MI') || 
            CASE WHEN HOUR(segment_start) < 12 OR HOUR(segment_start) = 24 THEN 'AM' ELSE 'PM' END || 
            ' - ' || 
            TO_CHAR(segment_end, 'HH12:MI') || 
            CASE WHEN HOUR(segment_end) < 12 OR HOUR(segment_end) = 24 THEN 'AM' ELSE 'PM' END,
            ', '
        ) WITHIN GROUP (ORDER BY segment_start) AS actual_availability
    FROM (
        SELECT
            provider_detail,
            coral_provider_id,
            calendar_date,
            time_slot,
            slot_start,
            slot_end,
            segment_start,
            segment_end
        FROM (
            -- Generate all potential segment boundaries (slot start, slot end, and all busy start/end times)
            WITH all_boundaries AS (
                SELECT
                    provider_detail,
                    coral_provider_id,
                    calendar_date,
                    time_slot,
                    slot_start,
                    slot_end,
                    slot_start AS boundary_time,
                    'SLOT_START' AS boundary_type
                FROM availability_with_busy_slots
                WHERE slot_start IS NOT NULL
                
                UNION ALL
                
                SELECT
                    provider_detail,
                    coral_provider_id,
                    calendar_date,
                    time_slot,
                    slot_start,
                    slot_end,
                    slot_end AS boundary_time,
                    'SLOT_END' AS boundary_type
                FROM availability_with_busy_slots
                WHERE slot_end IS NOT NULL
                
                UNION ALL
                
                SELECT
                    provider_detail,
                    coral_provider_id,
                    calendar_date,
                    time_slot,
                    slot_start,
                    slot_end,
                    busy_start AS boundary_time,
                    'BUSY_START' AS boundary_type
                FROM availability_with_busy_slots
                WHERE busy_start IS NOT NULL
                
                UNION ALL
                
                SELECT
                    provider_detail,
                    coral_provider_id,
                    calendar_date,
                    time_slot,
                    slot_start,
                    slot_end,
                    busy_end AS boundary_time,
                    'BUSY_END' AS boundary_type
                FROM availability_with_busy_slots
                WHERE busy_end IS NOT NULL
            ),
            
            -- Order boundaries and determine if each point is inside a busy period
            ordered_boundaries AS (
                SELECT
                    provider_detail,
                    coral_provider_id,
                    calendar_date,
                    time_slot,
                    slot_start,
                    slot_end,
                    boundary_time,
                    boundary_type,
                    -- Check if this time point is within any busy slot
                    EXISTS (
                        SELECT 1
                        FROM availability_with_busy_slots bs
                        WHERE 
                            ab.provider_detail = bs.provider_detail
                            AND ab.calendar_date = bs.calendar_date
                            AND ab.time_slot = bs.time_slot
                            AND bs.busy_start <= ab.boundary_time
                            AND bs.busy_end > ab.boundary_time
                    ) AS is_busy
                FROM all_boundaries ab
            ),
            
            -- Create segments by pairing adjacent boundaries
            segments AS (
                SELECT
                    provider_detail,
                    coral_provider_id,
                    calendar_date,
                    time_slot,
                    slot_start,
                    slot_end,
                    boundary_time AS segment_start,
                    LEAD(boundary_time) OVER (
                        PARTITION BY provider_detail, calendar_date, time_slot
                        ORDER BY boundary_time
                    ) AS segment_end,
                    is_busy
                FROM ordered_boundaries
            )
            
            -- Keep only available segments within the original slot
            SELECT
                provider_detail,
                coral_provider_id,
                calendar_date,
                time_slot,
                slot_start,
                slot_end,
                GREATEST(segment_start, slot_start) AS segment_start,
                LEAST(segment_end, slot_end) AS segment_end
            FROM segments
            WHERE 
                is_busy = FALSE
                AND segment_end IS NOT NULL
                AND segment_start < segment_end
                AND segment_end > slot_start
                AND segment_start < slot_end
        )
    )
    GROUP BY
        provider_detail,
        coral_provider_id,
        calendar_date,
        time_slot,
        slot_start,
        slot_end
)

-- Final output
SELECT 
    csh.provider_detail,
    csh.coral_provider_id,
    csh.calendar_date,
    csh.day_of_week,
    csh.week_number,
    csh.time_period,
    csh.time_slot,
    COALESCE(aas.actual_availability, csh.time_slot) AS actual_availability,
    COALESCE(bh.total_busy_hours, 0) AS total_busy_hours,
    csh.total_available_hours,
    
    -- Net available hours (available minus overlapping busy hours)
    GREATEST(csh.total_available_hours - COALESCE(obh.total_overlapping_busy_hours, 0), 0) AS net_available_hours,
    
    -- Original segment hours (from calculated slot)
    csh.morning_hours,
    csh.afternoon_hours,
    csh.evening_hours,
    
    -- Total busy hours by segment (regardless of template)
    COALESCE(bh.morning_busy_hours, 0) AS morning_busy_hours,
    COALESCE(bh.afternoon_busy_hours, 0) AS afternoon_busy_hours,
    COALESCE(bh.evening_busy_hours, 0) AS evening_busy_hours,
    
    -- Overlapping busy hours by segment (only within template)
    COALESCE(obh.morning_overlapping_busy_hours, 0) AS morning_overlapping_busy_hours,
    COALESCE(obh.afternoon_overlapping_busy_hours, 0) AS afternoon_overlapping_busy_hours,
    COALESCE(obh.evening_overlapping_busy_hours, 0) AS evening_overlapping_busy_hours,
    
    -- Net available hours by segment (available minus overlapping busy hours)
    GREATEST(csh.morning_hours - COALESCE(obh.morning_overlapping_busy_hours, 0), 0) AS net_morning_hours,
    GREATEST(csh.afternoon_hours - COALESCE(obh.afternoon_overlapping_busy_hours, 0), 0) AS net_afternoon_hours,
    GREATEST(csh.evening_hours - COALESCE(obh.evening_overlapping_busy_hours, 0), 0) AS net_evening_hours,
    
    -- Add a slot_id to help with aggregation in downstream models
    ROW_NUMBER() OVER (PARTITION BY csh.provider_detail, csh.calendar_date ORDER BY csh.time_slot) AS slot_id
FROM calculated_slot_hours csh
LEFT JOIN busy_hours_by_day bh ON 
    csh.provider_detail = bh.provider_detail 
    AND csh.calendar_date = bh.busy_date
LEFT JOIN overlapping_busy_hours obh ON
    csh.provider_detail = obh.provider_detail
    AND csh.calendar_date = obh.calendar_date
    AND csh.time_slot = obh.time_slot
LEFT JOIN actual_availability_segments aas ON
    csh.provider_detail = aas.provider_detail
    AND csh.calendar_date = aas.calendar_date
    AND csh.time_slot = aas.time_slot
WHERE csh.coral_provider_id IS NOT NULL 
ORDER BY 
    csh.provider_detail, 
    csh.calendar_date,
    slot_id