select 
provider_insurance_id, 
modified_date,
created_date,
created_by,
provider_id,
provider_user_id,
insurance_id,
status AS provider_insurance_status, 
status_last_updated_at,
status_last_updated_by,
activate_on_date,
processed_timestamp 
from {{ source('bubble', 'provider_insurance') }}