select 
provider.coral_provider_id,
provider.provider_first_name, 
provider.provider_last_name,
provider.provider_email, 
provider.NPI_NUMBER, 
provider.provider_phone_number,
provider.address,
provider.city, 
provider.state, 
provider.postal_code,
provider.latitude, 
provider.longitude, 
provider.travel_radius, 
provider.spoken_languages,
provider.provider_specialty,
provider.provider_product_status as provider_status, --changed the name to provide better clarity on source
provider.insurances_accepted_names, 
provider.provider_states_practicing, 
provider.estimated_hours_per_week, 
provider.first_login_date, 
provider.last_login_date, 
session_provider.first_session_date,
session_provider.most_recent_session_date,
session_provider.Completed_Last_30_Days,
session_provider.unique_patient_count_Last_30_Days,
session_provider.upcoming_session_date,
session_provider.farthest_out_session_date,
session_provider.total_completed_session_count,
provider.bubble_provider_availability_status,
provider_product_status,
provider_lifecycle_status
FROM {{ref('dim__provider')}} AS provider
LEFT JOIN {{ref('int__session_provider')}} AS session_provider
ON provider.coral_provider_id = session_provider.coral_provider_id 
WHERE 
provider.bubble_provider_ID is not null