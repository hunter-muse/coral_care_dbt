SELECT 
    parent.user_reference as parent_user_reference, 
    parent.parent_id,
    user.user_id as user_ID,
    user.first_name as parent_first_name,
    user.last_name as parent_last_name,
    user.email as parent_email,
    user.Phone,
    user.location_address as parent_location_address,
    user.address,
    user.city,
    user.state,
    user.postal_code,
    user.location_lat as parent_location_lat,
    user.location_lng as parent_location_lng,
    user.last_login_date as parent_last_login_date, 
    user.created_date as parent_first_login_date,
    LISTAGG(DISTINCT insurance.insurance_name, ', ') WITHIN GROUP (ORDER BY insurance.insurance_name) as insurance_providers
from {{ ref('stg__bubble__parent') }} as parent
LEFT JOIN {{ ref('stg__bubble__user') }} as user
    on parent.user_reference = user.user_id
LEFT JOIN {{ref('stg__bubble__parent_insurance')}} as parent_insurance
    on parent.parent_id = parent_insurance.parent_reference 
LEFT JOIN {{ref('stg__bubble__insurance')}} as insurance
    on parent_insurance.insurance_provider = insurance.insurance_id 
WHERE 
    user.last_name NOT ILIKE '%test%'
    AND user.email not ILIKE '%joincoral%'
    AND user.email not ILIKE '%pronoco%'
    AND user.email not ILIKE '%+test%'
    AND user.last_name not ILIKE '%parent%'
GROUP BY 
    parent.user_reference,
    parent.parent_id,
    user.user_id,
    user.first_name,
    user.last_name,
    user.email,
    user.Phone,
    user.location_address,
    user.address,
    user.city,
    user.state,
    user.postal_code,
    user.location_lat,
    user.location_lng,
    user.last_login_date,
    user.created_date
ORDER BY 1,2