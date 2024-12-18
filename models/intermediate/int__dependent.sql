select 
parent_id,
dependent_id,
dependent_name,
dob as dependent_dob,
gender as dependent_gender
from {{ ref('stg__bubble__dependent') }} as dependent   