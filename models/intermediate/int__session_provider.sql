with provider_summary as (
select 
    session.coral_provider_id,
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
ON session.coral_provider_id = provider.coral_provider_id
),

session_numbers as (
    select 
        *,
        ROW_NUMBER() OVER (PARTITION BY coral_provider_id ORDER BY session_start_date) as session_number
    from provider_summary
    where session_status = 'Completed'
),

milestone_sessions as (
    select 
        coral_provider_id,
        MAX(CASE WHEN session_number = 1 THEN session_start_date END) as first_session_complete_ts,
        MAX(CASE WHEN session_number = 5 THEN session_start_date END) as fifth_session_complete_ts,
        MAX(CASE WHEN session_number = 25 THEN session_start_date END) as twenty_fifth_session_complete_ts
    from session_numbers
    group by coral_provider_id
),

final as (
select 
coral_provider_id, 
    MIN(CASE WHEN rs.session_status IN ('Completed') THEN rs.session_start_date END) AS first_session_date,
    MAX(CASE WHEN rs.session_status = 'Completed' AND  rs.session_start_date < CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as most_recent_session_date,
    MIN(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as upcoming_session_date,
    MAX(CASE WHEN rs.session_status = 'Confirmed' AND  rs.session_start_date >= CURRENT_DATE() THEN rs.session_start_date ELSE NULL END) as farthest_out_session_date,
    COUNT(CASE WHEN rs.session_status IN ('Completed', 'Confirmed') THEN 1 ELSE NULL END) as total_completed_session_count,
    COUNT(CASE WHEN rs.session_status IN ('Completed', 'Confirmed') AND  days_since_session > -30 THEN 1 ELSE NULL END) as Completed_Last_30_Days,
    COUNT(DISTINCT CASE WHEN rs.session_status IN ('Completed', 'Confirmed') AND  days_since_session > -30 THEN dependent_id ELSE NULL END) as Unique_Patient_Count_Last_30_Days
from provider_summary AS rs
GROUP BY 1
)

select 
    f.coral_provider_id,
    f.total_completed_session_count,
    f.Completed_Last_30_Days,
    f.Unique_Patient_Count_Last_30_Days,
    CAST(f.first_session_date AS DATE) as first_session_date,
    CAST(f.most_recent_session_date AS DATE) as most_recent_session_date,
    CAST(f.upcoming_session_date AS DATE) as upcoming_session_date,
    CAST(f.farthest_out_session_date AS DATE) as farthest_out_session_date,
    CAST(m.first_session_complete_ts AS DATE) as first_session_complete_ts,
    CAST(m.fifth_session_complete_ts AS DATE) as fifth_session_complete_ts,
    CAST(m.twenty_fifth_session_complete_ts AS DATE) as twenty_fifth_session_complete_ts
from final f
LEFT JOIN milestone_sessions m ON f.coral_provider_id = m.coral_provider_id