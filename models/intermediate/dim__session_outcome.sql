select 
session_id,
provider_specialty,
date_of_service, 
goals_long_term_timeline, 
Treatment_plan_recommendations, 
recommendation 
from {{ ref('stg__google_sheets__session_outcome') }}