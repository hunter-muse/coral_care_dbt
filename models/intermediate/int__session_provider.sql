with provider_summary as (
select 
    session.provider_id,
    provider.provider_first_name,
    provider.provider_last_name,
    provider.Provider_Specialty AS provider_type,
    session.dependent_id, 
    session.session_start_date,
    session.session_status,
    session.session_format AS session_type,
    DATEDIFF(DAY, session_start_date, CURRENT_DATE()) AS days_since_session
from {{ref('int__session')}} AS session
LEFT JOIN {{ref('int__provider')}} AS provider
ON session.provider_id = provider.provider_id
),

final as (
select 
provider_id, 
    MIN(CASE WHEN rs.session_status IN ('Completed') THEN rs.session_start_date END) AS first_session_date,

    -- Most recent session details
    MAX(CASE WHEN rs.session_status = 'Completed' AND  rs.session_start_date < CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as most_recent_session_date,

    -- Upcoming session details
    MIN(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as upcoming_session_date,
    
    -- Farthest out session details
    MAX(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as farthest_out_session_date,
    
    -- Other aggregates
    COUNT(CASE WHEN rs.session_status IN ('Completed', 'Confirmed') THEN 1 ELSE NULL END) as total_completed_session_count,
    COUNT(CASE WHEN rs.session_status IN ('Completed', 'Confirmed') AND  days_since_session > -30 THEN 1 ELSE NULL END) as Completed_Last_30_Days,
    COUNT(DISTINCT CASE WHEN rs.session_status IN ('Completed', 'Confirmed') AND  days_since_session > -30 THEN dependent_id ELSE NULL END) as Unique_Patient_Count_Last_30_Days
from provider_summary AS rs
GROUP BY 1
)

select 
    provider_id,
    total_completed_session_count,
    Completed_Last_30_Days,
    Unique_Patient_Count_Last_30_Days,
    CAST(first_session_date AS DATE) as first_session_date,
    CAST(most_recent_session_date AS DATE) as most_recent_session_date,
    CAST(upcoming_session_date AS DATE) as upcoming_session_date,
    CAST(farthest_out_session_date AS DATE) as farthest_out_session_date
from final
