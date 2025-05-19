with base as (
SELECT 
p.coral_provider_id,
s.session_start_date 
FROM 
{{ref('int__provider')}} p 
join 
{{ref('int__session')}} s
on p.coral_provider_id = s.coral_provider_id
where 
s.session_status IN ('Completed', 'Confirmed')
) 

select 
coral_provider_id, 
MIN(CASE WHEN session_start_date < current_date() then session_start_date else null end) as last_completed_confirmed_appt, 
MAX(CASE WHEN session_start_date < current_date() then session_start_date else null end) as most_recent_session_date, 
MIN(CASE WHEN session_start_date >= current_date() then session_start_date else null end) as next_upcoming_appt, 
MIN(CASE WHEN session_start_date < current_date() then session_start_date else null end) as first_completed_appointment, 
MAX(CASE WHEN session_start_date >= current_date() then session_start_date else null end) as furthest_upcoming_session,
COUNT(*) as total_completed_confirmed_sessions
from 
base 
group by 1