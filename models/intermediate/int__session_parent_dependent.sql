WITH ranked_sessions AS (
    SELECT 
        session.*,
        -- Rank sessions for each type to help identify which attributes to pull
        ROW_NUMBER() OVER (
            PARTITION BY session.dependent_id, session.parent_id
            ORDER BY session.session_start_date DESC
        ) as recent_rank,
        ROW_NUMBER() OVER (
            PARTITION BY session.dependent_id, session.parent_id
            ORDER BY session.session_start_date ASC
        ) as first_session_rank,
        ROW_NUMBER() OVER (
            PARTITION BY session.dependent_id, session.parent_id
            ORDER BY CASE 
                WHEN session.session_start_date >= CURRENT_DATE() 
                THEN session.session_start_date 
                ELSE DATE '9999-12-31' 
            END ASC
        ) as upcoming_rank,
        ROW_NUMBER() OVER (
            PARTITION BY session.dependent_id, session.parent_id
            ORDER BY CASE 
                WHEN session.session_start_date >= CURRENT_DATE() 
                THEN session.session_start_date 
                ELSE NULL 
            END DESC
        ) as farthest_rank,
        COALESCE(session.provider_name, CONCAT(provider.provider_first_name, ' ', provider.provider_last_name)) as provider_name_combined 
    FROM {{ref('int__session')}} AS session
    LEFT JOIN {{ref('int__provider')}} AS provider
    ON session.coral_provider_id = provider.coral_provider_id 
),

-- Add milestone session numbers
session_numbers AS (
    SELECT 
        dependent_id,
        parent_id,
        session_start_date,
        ROW_NUMBER() OVER (PARTITION BY dependent_id, parent_id ORDER BY session_start_date) as session_number
    FROM ranked_sessions
    WHERE session_status = 'Completed'
),

milestone_sessions AS (
    SELECT 
        dependent_id,
        parent_id,
        MAX(CASE WHEN session_number = 1 THEN session_start_date END) as first_session_complete_ts,
        MAX(CASE WHEN session_number = 5 THEN session_start_date END) as fifth_session_complete_ts,
        MAX(CASE WHEN session_number = 25 THEN session_start_date END) as twenty_fifth_session_complete_ts
    FROM session_numbers
    GROUP BY dependent_id, parent_id
), 

final as (
    SELECT 
        rs.dependent_id, 
        rs.parent_id,
        MIN(CASE WHEN rs.session_status IN ('Completed', 'Confirmed') THEN rs.payment_type END) as payment_type,
        MIN(CASE WHEN rs.session_status IN ('Completed', 'Confirmed') THEN rs.reason_for_visit END) as reason_for_visit,
        MIN(CASE WHEN rs.session_status IN ('Completed') THEN rs.session_start_date END) AS first_session_date,
        MIN(CASE WHEN rs.session_status IN ('Completed') THEN rs.session_frequency END) AS first_session_frequency,
        -- Most recent session details
        MAX(CASE WHEN rs.session_status = 'Completed' AND rs.session_start_date < CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as most_recent_session_date,
        MAX(CASE WHEN rs.session_status = 'Completed' AND rs.session_start_date < CURRENT_DATE() THEN rs.session_format END) as most_recent_appt_type,
        MAX(CASE WHEN rs.session_status = 'Completed' AND rs.session_start_date < CURRENT_DATE() THEN rs.provider_type END) as most_recent_provider_type,
        MAX(CASE WHEN rs.session_status = 'Completed' AND rs.session_start_date < CURRENT_DATE() THEN rs.provider_name_combined END) as most_recent_provider_name,
        -- Upcoming session details
        MIN(CASE WHEN rs.session_status = 'Confirmed' AND rs.session_start_date >= CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as upcoming_session_date,
        MIN(CASE WHEN rs.session_status = 'Confirmed' AND rs.session_start_date >= CURRENT_DATE() THEN rs.session_start_time_only ELSE NULL END) as upcoming_session_time,
        MIN(CASE WHEN rs.session_status = 'Confirmed' AND rs.session_start_date >= CURRENT_DATE() THEN rs.session_format END) as upcoming_appt_type,
        MIN(CASE WHEN rs.session_status = 'Confirmed' AND rs.session_start_date >= CURRENT_DATE() THEN rs.provider_name_combined END) as upcoming_provider_name,
        MIN(CASE WHEN rs.session_status = 'Confirmed' AND rs.session_start_date >= CURRENT_DATE() THEN rs.provider_type ELSE NULL END) as upcoming_provider_type,
        -- Farthest out session details
        MAX(CASE WHEN rs.session_status = 'Confirmed' AND rs.session_start_date >= CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as farthest_out_session_date,
        MAX(CASE WHEN rs.session_status = 'Confirmed' AND rs.session_start_date >= CURRENT_DATE() THEN rs.session_format END) as farthest_out_appt_type,
        MAX(CASE WHEN rs.session_status = 'Confirmed' AND rs.session_start_date >= CURRENT_DATE() THEN rs.provider_type END) as farthest_out_provider_type,
        -- Other aggregates
        COUNT(CASE WHEN rs.session_status IN ('Completed', 'Confirmed') THEN 1 ELSE NULL END) as completed_session_count
    FROM ranked_sessions rs
    LEFT JOIN {{ref('int__parent_dependent_crosswalk')}} AS parent_dependent
        ON rs.parent_id = parent_dependent.parent_id
    GROUP BY 1,2
) 

SELECT 
    f.parent_id, 
    f.dependent_id, 
    f.reason_for_visit, 
    f.payment_type, 
    CAST(f.first_session_date AS DATE) as first_session_date, 
    CAST(f.most_recent_session_date AS DATE) as most_recent_session_date, 
    CAST(f.upcoming_session_date AS DATE) as upcoming_session_date, 
    f.upcoming_session_time, 
    CAST(f.farthest_out_session_date AS DATE) as farthest_out_session_date, 
    f.most_recent_appt_type,
    f.upcoming_appt_type,
    f.farthest_out_appt_type,
    f.completed_session_count,
    f.most_recent_provider_name,
    f.most_recent_provider_type,
    f.upcoming_provider_name, 
    f.first_session_frequency,
    CAST(m.first_session_complete_ts AS DATE) as first_session_complete_ts,
    CAST(m.fifth_session_complete_ts AS DATE) as fifth_session_complete_ts,
    CAST(m.twenty_fifth_session_complete_ts AS DATE) as twenty_fifth_session_complete_ts,
    CASE
        WHEN f.first_session_date IS NULL
        AND f.completed_session_count = 0 THEN 1
        ELSE 0
    END AS NeverBookedFLG
FROM final f
LEFT JOIN milestone_sessions m 
    ON f.dependent_id = m.dependent_id 
    AND f.parent_id = m.parent_id