with prep as (
    select 
        dependent.dependent_id,
        dependent.dependent_name,
        SPLIT_PART(dependent.dependent_name, ' ', 1) as dependent_first_name,
        SPLIT_PART(dependent.dependent_name, ' ', -1) as dependent_last_name,
        dependent.dob as dependent_dob,
        dependent.gender as dependent_gender,

        outcomes.evaluation_summary_from_session,
        outcomes.treatment_plan_recommendation_from_session,
        outcomes.evaluation_recommendation_from_session,
        outcomes.goals_long_term_timeline_from_session,
        outcomes.evaluation_summary_from_evaluation,
        outcomes.treatment_plan_recommendation_from_evaluation,
        outcomes.evaluation_recommendation,
        outcomes.goals_long_term_timeline,
        outcomes.most_recent_session_date,
        outcomes.most_recent_evaluation_date,
        outcomes.most_recent_session_id,
        outcomes.most_recent_evaluation_id,

        CASE 
            WHEN DATEDIFF('month', dependent.dob, CURRENT_DATE()) < 12 THEN 
                DATEDIFF('month', dependent.dob, CURRENT_DATE())
            ELSE 
                DATEDIFF('year', dependent.dob, CURRENT_DATE())
        END as dependent_age_number,
        CASE 
            WHEN DATEDIFF('month', dependent.dob, CURRENT_DATE()) < 12 THEN 'months'
            ELSE 'years'
        END as dependent_age_unit,
        parent.coral_parent_id
    from {{ ref('stg__bubble__dependent') }} as dependent
    left join {{ ref('int__parent') }} as parent
        on dependent.parent_id = parent.bubble_parent_user_id
    left join {{ ref('int__recent_outcome') }} outcomes 
        ON outcomes.dependent_id = dependent.dependent_id 
) 

select 
    coral_parent_id,
    dependent_id,
    dependent_name,
    dependent_first_name,
    CASE WHEN dependent_last_name = dependent_first_name THEN NULL ELSE dependent_last_name END as dependent_last_name,
    dependent_dob,
    dependent_gender,
    dependent_age_number,
    dependent_age_unit,
    evaluation_summary_from_session,
    treatment_plan_recommendation_from_session,
    evaluation_recommendation_from_session,
    goals_long_term_timeline_from_session,
    evaluation_summary_from_evaluation,
    treatment_plan_recommendation_from_evaluation,
    evaluation_recommendation,
    goals_long_term_timeline,
    most_recent_session_date,
    most_recent_evaluation_date,
    most_recent_session_id,
    most_recent_evaluation_id
from prep