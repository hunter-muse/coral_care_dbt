select 
    session_parent_dependent.parent_id,
    MAX(session_parent_dependent.reason_for_visit) as reason_for_visit, 
    MAX(session_parent_dependent.payment_type) as payment_type, 
    MAX(session_parent_dependent.most_recent_session_date) as most_recent_session_date,
    MIN(session_parent_dependent.upcoming_session_date) as upcoming_session_date,
    MIN(session_parent_dependent.upcoming_session_time) as upcoming_session_time,
    MAX(session_parent_dependent.farthest_out_session_date) as farthest_out_session_date,
    MAX(session_parent_dependent.most_recent_provider_name) as most_recent_provider_name,
    MAX(session_parent_dependent.upcoming_provider_name) as upcoming_provider_name,
    SUM(session_parent_dependent.completed_session_count) as session_count
from {{ref('int__session_parent_dependent')}} AS session_parent_dependent
GROUP BY 1
