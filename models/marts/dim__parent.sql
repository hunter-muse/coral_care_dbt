select 
ip.*, 
ips.total_completed_confirmed_sessions, 
ips.first_completed_confirmed_appt, 
ips.last_completed_confirmed_appt, 
ips.last_completed_confirmed_appt_format, 
ips.next_upcoming_appt, 
ips.furthest_upcoming_session
from {{ ref('int__parent') }} ip 
left join {{ ref('int__parent_scorecard') }} ips on ip.coral_parent_id = ips.coral_parent_id