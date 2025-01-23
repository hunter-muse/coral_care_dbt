select 
availability_id, 
active, 
created_by, 
availability.created_date, 
duration, 
end_time, 
modified_date, 
number_120, 
number_30, 
number_45, 
number_60, 
number_75, 
number_90, 
offset, 
provider_detail AS bubble_provider_id, 
start_time
from {{ ref('stg__bubble__availability') }} as availability
    