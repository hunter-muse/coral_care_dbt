select
parent.user_reference as parent_ID, 
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
user.created_date as parent_first_login_date
from {{ ref('stg__bubble__parent') }} as parent
LEFT JOIN {{ ref('stg__bubble__user') }} as user
on parent.user_reference = user.user_id
WHERE 
user.last_name NOT ILIKE '%test%'
AND 
user.email not ILIKE '%joincoral%'
AND 
user.email not ILIKE '%pronoco%'
AND user.email not ILIKE '%+test%'
AND user.last_name not ILIKE '%parent%'