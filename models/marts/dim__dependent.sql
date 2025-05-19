select 
id.*, 
ids.total_completed_confirmed_sessions, 
ids.first_completed_confirmed_appt, 
ids.last_completed_confirmed_appt, 
ids.last_completed_confirmed_appt_format, 
ids.next_upcoming_appt, 
ids.furthest_upcoming_session
from {{ ref('int__dependent') }} id 
left join {{ ref('int__dependent_scorecard') }} ids on id.dependent_id = ids.dependent_id