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
), 

ranked_dependents AS (
    SELECT 
        spd.parent_id,
        d.dependent_name,
        d.dependent_first_name,
        d.dependent_last_name,
        d.dependent_dob,
        d.dependent_gender,
        d.dependent_age_number,
        d.dependent_age_unit,
        FIRST_VALUE(spd.reason_for_visit) OVER (
            PARTITION BY spd.parent_id, spd.dependent_id 
            ORDER BY spd.first_session_date
        ) as dependent_reason_for_visit,
        ROW_NUMBER() OVER (
            PARTITION BY spd.parent_id 
            ORDER BY spd.first_session_date
        ) as dependent_number
    FROM {{ref('int__session_parent_dependent')}} AS spd
    LEFT JOIN {{ref('int__dependent')}} AS d
        ON spd.dependent_id = d.dependent_id
    QUALIFY dependent_number <= 3  -- Limit to 3 dependents per parent
),

flattened_dependents AS (
    SELECT 
        parent_id,
        MAX(CASE WHEN dependent_number = 1 THEN dependent_name END) as dependent_1_name,
        MAX(CASE WHEN dependent_number = 1 THEN dependent_first_name END) as dependent_1_first_name,
        MAX(CASE WHEN dependent_number = 1 THEN dependent_last_name END) as dependent_1_last_name,
        MAX(CASE WHEN dependent_number = 1 THEN dependent_dob END) as dependent_1_dob,
        MAX(CASE WHEN dependent_number = 1 THEN dependent_gender END) as dependent_1_gender,
        MAX(CASE WHEN dependent_number = 1 THEN dependent_reason_for_visit END) as dependent_1_reason_for_visit,
        
        MAX(CASE WHEN dependent_number = 2 THEN dependent_name END) as dependent_2_name,
        MAX(CASE WHEN dependent_number = 2 THEN dependent_first_name END) as dependent_2_first_name,
        MAX(CASE WHEN dependent_number = 2 THEN dependent_last_name END) as dependent_2_last_name,  
        MAX(CASE WHEN dependent_number = 2 THEN dependent_dob END) as dependent_2_dob,
        MAX(CASE WHEN dependent_number = 2 THEN dependent_gender END) as dependent_2_gender,
        MAX(CASE WHEN dependent_number = 2 THEN dependent_reason_for_visit END) as dependent_2_reason_for_visit,
        
        MAX(CASE WHEN dependent_number = 3 THEN dependent_name END) as dependent_3_name,
        MAX(CASE WHEN dependent_number = 3 THEN dependent_first_name END) as dependent_3_first_name,
        MAX(CASE WHEN dependent_number = 3 THEN dependent_last_name END) as dependent_3_last_name,
        MAX(CASE WHEN dependent_number = 3 THEN dependent_dob END) as dependent_3_dob,
        MAX(CASE WHEN dependent_number = 3 THEN dependent_gender END) as dependent_3_gender,
        MAX(CASE WHEN dependent_number = 3 THEN dependent_reason_for_visit END) as dependent_3_reason_for_visit
    FROM ranked_dependents
    GROUP BY parent_id
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
    END AS engagement_status,
    -- Add dependent information
    fd.dependent_1_name,
    fd.dependent_1_first_name,
    fd.dependent_1_last_name,
    fd.dependent_1_dob,
    fd.dependent_1_gender,
    fd.dependent_1_reason_for_visit,
    
    fd.dependent_2_name,
    fd.dependent_2_first_name,
    fd.dependent_2_last_name,
    fd.dependent_2_dob,
    fd.dependent_2_gender,
    fd.dependent_2_reason_for_visit,
    
    fd.dependent_3_name,
    fd.dependent_3_first_name,
    fd.dependent_3_last_name,
    fd.dependent_3_dob,
    fd.dependent_3_gender,
    fd.dependent_3_reason_for_visit
FROM parent_summary parent_summary
LEFT JOIN flattened_dependents fd
    ON parent_summary.parent_id = fd.parent_id
