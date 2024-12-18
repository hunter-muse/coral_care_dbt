select distinct 
parent.parent_id,
parent.parent_first_name,
parent.parent_last_name,
dependent.dependent_id,
dependent.dependent_name,
dependent.dependent_dob,
dependent.dependent_gender
from 
{{ref('int__parent')}} parent 
LEFT JOIN 
{{ref('int__dependent')}} dependent
on parent.parent_id = dependent.parent_id
where dependent.dependent_name is not null
order by parent.parent_id


