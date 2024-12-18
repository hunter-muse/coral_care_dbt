with prep as (
    select 
    parent_id,
    dependent_id,
    dependent_name,
    SPLIT_PART(dependent_name, ' ', 1) as dependent_first_name,
    SPLIT_PART(dependent_name, ' ', -1) as dependent_last_name,
    dob as dependent_dob,
    gender as dependent_gender,
    CASE 
        WHEN DATEDIFF('month', dob, CURRENT_DATE()) < 12 THEN 
            DATEDIFF('month', dob, CURRENT_DATE())
        ELSE 
            DATEDIFF('year', dob, CURRENT_DATE())
    END as dependent_age_number,
    CASE 
        WHEN DATEDIFF('month', dob, CURRENT_DATE()) < 12 THEN 'months'
        ELSE 'years'
    END as dependent_age_unit
from {{ ref('stg__bubble__dependent') }} as dependent
where dependent_name NOT ILIKE 'test%'
and dependent_name NOT ILIKE '%test'
and dependent_name NOT ILIKE 'kiddo%'
and dependent_name NOT ILIKE 'fakechild%'
and dependent_name NOT ILIKE '%deprecated%'
and dependent_name NOT ILIKE '%snowperson%'
) 

select 
parent_id,
dependent_id,
dependent_name,
dependent_first_name,
CASE WHEN dependent_last_name = dependent_first_name THEN NULL ELSE dependent_last_name END as dependent_last_name,
dependent_dob,
dependent_gender,
dependent_age_number,
dependent_age_unit
from prep