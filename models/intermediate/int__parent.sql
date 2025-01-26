 with bubble_parents as (
    SELECT user.*, parent.parent_id
    FROM {{ ref('stg__bubble__parent') }} as parent
    LEFT JOIN {{ ref('stg__bubble__user') }} as user
        on parent.user_reference = user.user_id
 ),   

    
parent_join as (
    SELECT 
    MD5(COALESCE(hubspot.record_id::VARCHAR, '') || COALESCE(bubble.user_id::VARCHAR, '')) as coral_parent_id,
    COALESCE(bubble.first_name, hubspot.parent_first_name) as parent_first_name,
    COALESCE(bubble.last_name, hubspot.parent_last_name) as parent_last_name,
    hubspot.record_id as hubspot_parent_id,
    bubble.user_id as bubble_parent_user_id,
    bubble.parent_id as bubble_parent_id, 
    COALESCE(bubble.email, hubspot.parent_email) as parent_email,
    COALESCE(bubble.Phone, hubspot.parent_phone) as parent_phone,
    bubble.location_address as parent_location_address,
    --COALESCE(bubble.location_address, hubspot.parent_location_address) as parent_location_address,
    COALESCE(bubble.address, hubspot.street_address) as parent_address,
    COALESCE(bubble.city, hubspot.city) as parent_city,
    COALESCE(bubble.state, hubspot.state) as parent_state,
    COALESCE(bubble.postal_code, hubspot.zip_code) as parent_postal_code,
    COALESCE(bubble.location_lat, hubspot.latitude) as parent_location_lat,
    COALESCE(bubble.location_lng, hubspot.longitude) as parent_location_lng,
    COALESCE(bubble.last_login_date, hubspot.last_login_date) as parent_last_login_date, 
    hubspot.provider_type AS parent_provider_type_needed, 
    bubble.created_date as parent_first_login_date
    --COALESCE(bubble.created_date, hubspot.created_date) as parent_first_login_date,
    FROM bubble_parents bubble
    FULL OUTER JOIN {{ref('stg__hubspot__contact_parent')}} as hubspot
        on TRIM(lower(bubble.first_name)) = TRIM(lower(hubspot.parent_first_name))
        and TRIM(lower(bubble.last_name)) = TRIM(lower(hubspot.parent_last_name))
)
    
select parent_join.*,    
LISTAGG(DISTINCT insurance.insurance_name, ', ') WITHIN GROUP (ORDER BY insurance.insurance_name) as insurance_providers 
from parent_join
LEFT JOIN {{ref('stg__bubble__parent_insurance')}} as parent_insurance
on parent_join.bubble_parent_id = parent_insurance.parent_reference 
LEFT JOIN {{ref('stg__bubble__insurance')}} as insurance
on parent_insurance.insurance_provider = insurance.insurance_id 
GROUP BY ALL
