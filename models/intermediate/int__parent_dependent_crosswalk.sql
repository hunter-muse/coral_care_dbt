select distinct 
parent.coral_parent_id,
parent.parent_first_name,
parent.parent_last_name,
dependent.dependent_id,
dependent.dependent_name,
dependent.dependent_first_name,
dependent.dependent_last_name,
dependent.dependent_dob,
dependent.dependent_gender,
dependent.evaluation_summary_from_session,
dependent.treatment_plan_recommendation_from_session,
dependent.evaluation_recommendation_from_session,
dependent.goals_long_term_timeline_from_session,
dependent.evaluation_summary_from_evaluation,
dependent.treatment_plan_recommendation_from_evaluation,
dependent.evaluation_recommendation,
dependent.goals_long_term_timeline,
dependent.most_recent_session_date,
dependent.most_recent_evaluation_date,
dependent.most_recent_session_id,
dependent.most_recent_evaluation_id
from 
{{ref('int__parent')}} parent 
LEFT JOIN 
{{ref('int__dependent')}} dependent
on parent.coral_parent_id = dependent.coral_parent_id
where dependent.dependent_name is not null
order by parent.coral_parent_id


