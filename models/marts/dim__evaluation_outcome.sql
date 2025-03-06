select 
session_id, 
provider_specialty,
date_of_service, 
evaluation_summary, 
Treatment_plan_recommendations, 
recommendation 
from 
{{ ref('stg__google_sheets__evaluation_outcome') }}