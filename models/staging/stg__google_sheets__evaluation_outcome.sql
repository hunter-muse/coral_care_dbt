select 
provider_specialty,
to_char(to_date(date_of_service, 'MM/DD/YY'), 'MM-DD-YYYY') as date_of_service,
evaluation_summary,
session_id, 
treatment_plan_recommendations,
recommendation
from {{ source('google_sheets', 'evaluation_outcome') }}