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
        provider.provider_Specialty as provider_type,
        COALESCE(session.provider_name, CONCAT(provider.provider_first_name, ' ', provider.provider_last_name)) as provider_name_combined 
    FROM {{ref('int__session')}} AS session
    LEFT JOIN {{ref('int__provider')}} AS provider
    ON session.provider_id = provider.user_provider_id
), 

final as (
SELECT 
    rs.dependent_id, 
    rs.parent_id,
    MIN(CASE WHEN rs.session_status IN ('Completed', 'Confirmed') THEN rs.payment_type END) as payment_type,
    MIN(CASE WHEN rs.session_status IN ('Completed', 'Confirmed') THEN rs.reason_for_visit END) as reason_for_visit,
    MIN(CASE WHEN rs.session_status = 'Completed' AND rs.session_start_date <= CURRENT_DATE() THEN rs.session_start_date END) AS first_session_date,

    -- Most recent session details
    MAX(CASE WHEN rs.session_status = 'Completed' AND  rs.session_start_date < CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as most_recent_session_date,
    MAX(CASE WHEN rs.session_status = 'Completed' AND  rs.recent_rank = 1 AND rs.session_start_date < CURRENT_DATE() THEN rs.session_format END) as most_recent_appt_type,
    --MAX(CASE WHEN rs.recent_rank = 1 AND rs.session_start_date < CURRENT_DATE() THEN rs.product_appt_frequency END) as most_recent_frequency,
    MAX(CASE WHEN rs.session_status = 'Completed' AND  rs.recent_rank = 1 AND rs.session_start_date < CURRENT_DATE() THEN rs.provider_type END) as most_recent_provider_type,
    MAX(CASE WHEN rs.session_status = 'Completed' AND  rs.recent_rank = 1 AND rs.session_start_date < CURRENT_DATE() THEN rs.provider_name_combined END) as most_recent_provider_name,

    -- Upcoming session details
    MIN(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as upcoming_session_date,
    MIN(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.session_start_time_only ELSE NULL END) as upcoming_session_time,
    MAX(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.session_format END) as upcoming_appt_type,
    MAX(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.provider_name_combined END) as upcoming_provider_name,
    --MAX(CASE WHEN rs.upcoming_rank = 1 THEN rs.product_appt_frequency END) as upcoming_frequency,
    MAX(CASE WHEN rs.session_status = 'Confirmed' AND  rs.upcoming_rank = 1 THEN rs.provider_type ELSE NULL END) as upcoming_provider_type,
    -- Farthest out session details
    MAX(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as farthest_out_session_date,
    MAX(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.session_format END) as farthest_out_appt_type,
    --MAX(CASE WHEN rs.farthest_rank = 1 THEN rs.product_appt_frequency END) as farthest_out_frequency,
    MAX(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.provider_type END) as farthest_out_provider_type,
    
    -- Other aggregates
    COUNT(CASE WHEN rs.session_status IN ('Completed', 'Confirmed') THEN 1 ELSE NULL END) as completed_session_count

FROM ranked_sessions rs
LEFT JOIN {{ref('int__parent_dependent_crosswalk')}} AS parent_dependent
    ON rs.parent_id = parent_dependent.parent_id
GROUP BY 1,2
) 

select 
parent_id, 
dependent_id, 
reason_for_visit, 
payment_type, 
CAST(first_session_date AS DATE) as first_session_date, 
CAST(most_recent_session_date AS DATE) as most_recent_session_date, 
CAST(upcoming_session_date AS DATE) as upcoming_session_date, 
upcoming_session_time, 
CAST(farthest_out_session_date AS DATE) as farthest_out_session_date, 
most_recent_appt_type,
upcoming_appt_type,
farthest_out_appt_type,
completed_session_count,
most_recent_provider_name,
upcoming_provider_name, 
CASE
        WHEN first_session_date IS NULL
        AND completed_session_count = 0 THEN 1
        ELSE 0
      END AS NeverBookedFLG
from 
final 
-- where parent_id = '1727521616734x862398024099603800'