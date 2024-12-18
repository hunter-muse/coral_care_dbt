with parent_summary as ( 
select 
    session_parent_dependent.parent_id,
    MAX(session_parent_dependent.reason_for_visit) as reason_for_visit, 
    MAX(session_parent_dependent.payment_type) as payment_type, 
    MAX(session_parent_dependent.first_session_date) as first_session_date,
    MAX(session_parent_dependent.first_session_frequency) as first_session_frequency,
    MAX(session_parent_dependent.most_recent_session_date) as most_recent_session_date,
    MIN(session_parent_dependent.upcoming_session_date) as upcoming_session_date,
    MIN(session_parent_dependent.upcoming_session_time) as upcoming_session_time,
    MAX(session_parent_dependent.farthest_out_session_date) as farthest_out_session_date,
    MAX(session_parent_dependent.most_recent_provider_name) as most_recent_provider_name,
    MAX(session_parent_dependent.most_recent_provider_type) as most_recent_provider_type,
    MAX(session_parent_dependent.upcoming_provider_name) as upcoming_provider_name,
    MAX(session_parent_dependent.most_recent_appt_type) as most_recent_appt_type,
    MAX(session_parent_dependent.upcoming_appt_type) as upcoming_appt_type,
    SUM(session_parent_dependent.completed_session_count) as session_count
from {{ref('int__session_parent_dependent')}} AS session_parent_dependent
GROUP BY 1
) 

select 
    parent_summary.parent_id,
    parent_summary.reason_for_visit,
    parent_summary.payment_type,
    parent_summary.first_session_date,
    parent_summary.first_session_frequency,
    parent_summary.most_recent_session_date,
    parent_summary.upcoming_session_date,
    parent_summary.upcoming_session_time,
    parent_summary.farthest_out_session_date,
    parent_summary.most_recent_provider_name,
    parent_summary.most_recent_provider_type,
    parent_summary.upcoming_provider_name,
    parent_summary.most_recent_appt_type,
    parent_summary.upcoming_appt_type,
    parent_summary.session_count,
    DATEDIFF(DAY, parent_summary.most_recent_session_date, CURRENT_DATE()) as days_since_last_session,
    CASE 
        WHEN parent_summary.upcoming_session_date IS NOT NULL THEN 'In Treatment'
        WHEN parent_summary.upcoming_session_date IS NULL AND DATEDIFF(DAY, parent_summary.most_recent_session_date, CURRENT_DATE()) <= 14 THEN 'Pre-Churn'
        WHEN parent_summary.upcoming_session_date IS NULL AND DATEDIFF(DAY, parent_summary.most_recent_session_date, CURRENT_DATE()) > 14 THEN 'Churned'
        WHEN parent_summary.session_count = 0 THEN 'No Treatment'
        ELSE 'Churned'
    END AS engagement_status
from parent_summary

