with base as (
SELECT 
d.dependent_id,
s.session_ID, 
s.session_start_date,
s.session_format 
FROM 
{{ref('int__dependent')}} d 
join 
{{ref('int__session')}} s
on d.dependent_id = s.dependent_id
where 
s.session_status IN ('Completed', 'Confirmed')
) 

select 
dependent_id, 
MIN(CASE WHEN session_start_date < current_date() then session_start_date else null end) as first_completed_confirmed_appt,
MAX(CASE WHEN session_start_date < current_date() then session_start_date else null end) as last_completed_confirmed_appt,  
MAX(CASE WHEN session_start_date < current_date() then session_format else null end) as last_completed_confirmed_appt_format,  
MIN(CASE WHEN session_start_date >= current_date() then session_start_date else null end) as next_upcoming_appt, 
MAX(CASE WHEN session_start_date >= current_date() then session_start_date else null end) as furthest_upcoming_session,
COUNT(*) as total_completed_confirmed_sessions
from 
base 
group by 1
