WITH numbered_dependents AS (
  SELECT 
    coral_parent_id,
    parent_first_name,
    parent_last_name,
    dependent_id,
    dependent_name,
    dependent_gender, 
    dependent_dob,
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
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN dependent_id END), 'No Dependent') as dependent_2_id,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN dependent_name END), 'No Dependent') as dependent_2_name,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN dependent_dob END), NULL) as dependent_2_dob,
    COALESCE(MAX(CASE WHEN dependent_number = 2 THEN dependent_gender END), 'No Dependent') as dependent_2_gender,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN dependent_id END), 'No Dependent') as dependent_3_id,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN dependent_name END), 'No Dependent') as dependent_3_name,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN dependent_dob END), NULL) as dependent_3_dob,
    COALESCE(MAX(CASE WHEN dependent_number = 3 THEN dependent_gender END), 'No Dependent') as dependent_3_gender,
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