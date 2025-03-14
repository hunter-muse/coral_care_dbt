WITH numbered_dependents AS (
  SELECT 
    coral_parent_id,
    parent_first_name,
    parent_last_name,
    dependent_id,
    dependent_name,
    dependent_gender, 
    dependent_dob,
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
    most_recent_evaluation_id,
    ROW_NUMBER() OVER (PARTITION BY coral_parent_id ORDER BY dependent_id) as dependent_number,
    COUNT(*) OVER (PARTITION BY coral_parent_id) as total_dependents
  FROM {{ ref('int__parent_dependent_crosswalk') }}
),

final_data AS (
  SELECT 
    coral_parent_id,
    parent_first_name,
    parent_last_name,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN dependent_id END), 'No Dependent') as dependent_1_id,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN dependent_name END), 'No Dependent') as dependent_1_name,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN dependent_dob END), NULL) as dependent_1_dob,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN dependent_gender END), 'No Dependent') as dependent_1_gender,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN evaluation_summary_from_session END), NULL) as dependent_1_evaluation_summary_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN treatment_plan_recommendation_from_session END), NULL) as dependent_1_treatment_plan_recommendation_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN evaluation_recommendation_from_session END), NULL) as dependent_1_evaluation_recommendation_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN goals_long_term_timeline_from_session END), NULL) as dependent_1_goals_long_term_timeline_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN evaluation_summary_from_evaluation END), NULL) as dependent_1_evaluation_summary_from_evaluation,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN treatment_plan_recommendation_from_evaluation END), NULL) as dependent_1_treatment_plan_recommendation_from_evaluation,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN evaluation_recommendation END), NULL) as dependent_1_evaluation_recommendation,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN goals_long_term_timeline END), NULL) as dependent_1_goals_long_term_timeline,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN most_recent_session_date END), NULL) as dependent_1_most_recent_session_date,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN most_recent_evaluation_date END), NULL) as dependent_1_most_recent_evaluation_date,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN most_recent_session_id END), NULL) as dependent_1_most_recent_session_id,
    COALESCE(MAX(CASE WHEN dependent_number = 1 THEN most_recent_evaluation_id END), NULL) as dependent_1_most_recent_evaluation_id,
    
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN dependent_id END), 'No Dependent') as dependent_2_id,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN dependent_name END), 'No Dependent') as dependent_2_name,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN dependent_dob END), NULL) as dependent_2_dob,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN dependent_gender END), 'No Dependent') as dependent_2_gender,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN evaluation_summary_from_session END), NULL) as dependent_2_evaluation_summary_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN treatment_plan_recommendation_from_session END), NULL) as dependent_2_treatment_plan_recommendation_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN evaluation_recommendation_from_session END), NULL) as dependent_2_evaluation_recommendation_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN goals_long_term_timeline_from_session END), NULL) as dependent_2_goals_long_term_timeline_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN evaluation_summary_from_evaluation END), NULL) as dependent_2_evaluation_summary_from_evaluation,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN treatment_plan_recommendation_from_evaluation END), NULL) as dependent_2_treatment_plan_recommendation_from_evaluation,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN evaluation_recommendation END), NULL) as dependent_2_evaluation_recommendation,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN goals_long_term_timeline END), NULL) as dependent_2_goals_long_term_timeline,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN most_recent_session_date END), NULL) as dependent_2_most_recent_session_date,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN most_recent_evaluation_date END), NULL) as dependent_2_most_recent_evaluation_date,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN most_recent_session_id END), NULL) as dependent_2_most_recent_session_id,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN most_recent_evaluation_id END), NULL) as dependent_2_most_recent_evaluation_id,
    
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN dependent_id END), 'No Dependent') as dependent_3_id,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN dependent_name END), 'No Dependent') as dependent_3_name,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN dependent_dob END), NULL) as dependent_3_dob,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN dependent_gender END), 'No Dependent') as dependent_3_gender,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN evaluation_summary_from_session END), NULL) as dependent_3_evaluation_summary_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN treatment_plan_recommendation_from_session END), NULL) as dependent_3_treatment_plan_recommendation_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN evaluation_recommendation_from_session END), NULL) as dependent_3_evaluation_recommendation_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN goals_long_term_timeline_from_session END), NULL) as dependent_3_goals_long_term_timeline_from_session,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN evaluation_summary_from_evaluation END), NULL) as dependent_3_evaluation_summary_from_evaluation,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN treatment_plan_recommendation_from_evaluation END), NULL) as dependent_3_treatment_plan_recommendation_from_evaluation,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN evaluation_recommendation END), NULL) as dependent_3_evaluation_recommendation,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN goals_long_term_timeline END), NULL) as dependent_3_goals_long_term_timeline,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN most_recent_session_date END), NULL) as dependent_3_most_recent_session_date,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN most_recent_evaluation_date END), NULL) as dependent_3_most_recent_evaluation_date,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN most_recent_session_id END), NULL) as dependent_3_most_recent_session_id,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN most_recent_evaluation_id END), NULL) as dependent_3_most_recent_evaluation_id,
    
    MAX(total_dependents) as number_of_dependents
  FROM numbered_dependents
  GROUP BY 
    coral_parent_id,
    parent_first_name,
    parent_last_name
)

SELECT 
  *,
  CASE 
    WHEN number_of_dependents = 0 THEN 'No dependents'
    WHEN number_of_dependents = 1 THEN 'Single dependent'
    WHEN number_of_dependents = 2 THEN 'Two dependents'
    ELSE 'Three or more dependents'
  END as dependent_status
FROM final_data
ORDER BY coral_parent_id