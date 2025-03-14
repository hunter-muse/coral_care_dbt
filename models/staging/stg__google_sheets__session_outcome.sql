select 
provider_specialty,
to_char(to_date(date_of_service, 'MM/DD/YY'), 'MM-DD-YYYY') as date_of_service, 
goals_long_term_timeline,
session_id, 
treatment_plan_recommendations, 
recommendation 
from {{ source('google_sheets', 'session_outcome') }}