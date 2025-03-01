select  
slot_id, 
created_by,
created_date, 
modified_date, 
block_appointment, 
delete_flag, 
duration_in_minutes, 
start_date,
end_date, 
provider_detail, 
session, 
session_type_name
from {{ source('bubble', 'busy_slot') }}