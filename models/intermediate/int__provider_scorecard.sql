with base as (
SELECT 
p.coral_provider_id,
s.session_ID, 
s.session_start_date,
s.session_format 
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
MAX(CASE WHEN session_start_date < current_date() then session_start_date else null end) as last_completed_confirmed_appt, 
MIN(CASE WHEN session_start_date < current_date() then session_start_date else null end) as first_completed_appointment,
MAX(CASE WHEN session_start_date < current_date() then session_format else null end) as last_completed_confirmed_appt_format,
MIN(CASE WHEN session_start_date >= current_date() then session_start_date else null end) as next_upcoming_appt, 
MAX(CASE WHEN session_start_date >= current_date() then session_start_date else null end) as furthest_upcoming_session,
COUNT(CASE WHEN session_format = 'Single Session' then session_ID else null end) as total_single_sessions, 
COUNT(CASE WHEN session_format = 'Single Session' then session_ID else null end) / COUNT(distinct session_ID) as single_session_completion_rate, 
COUNT(CASE WHEN session_format = 'Recurring' then session_ID else null end) as total_recurring_sessions, 
COUNT(CASE WHEN session_format = 'Recurring' then session_ID else null end) / COUNT(distinct session_ID) as recurring_session_completion_rate, 
COUNT(*) as total_completed_confirmed_sessions
from 
base 
group by 1