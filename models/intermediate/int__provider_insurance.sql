select 
p.coral_provider_id, 
p.provider_first_name, 
p.provider_last_name, 
i.insurance_name,
pi.*
from 
{{ ref('stg__bubble__provider_insurance') }} pi 
left join {{ ref('int__provider') }} p on pi.provider_id = p.bubble_provider_id 
left join {{ ref('stg__bubble__insurance') }} i on pi.insurance_id = i.insurance_id
