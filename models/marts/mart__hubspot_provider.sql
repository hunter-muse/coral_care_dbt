SELECT 
parent.parent_email,
parent.parent_id,
parent.parent_first_name,
parent.parent_last_name,
parent.address AS street_address,
parent.parent_location_LAT AS latitude, 
parent.parent_location_LNG AS longitude,
parent.city,
parent.state AS state_region_code,
parent.postal_code,
parent.phone,
session_parent.most_recent_provider_type AS Provider_Type,
session_parent.first_session_frequency AS appointment_frequency,
session_parent.most_recent_appt_type,
session_parent.upcoming_appt_type, 
session_parent.session_count,
/*added dependent fields here */
session_parent.payment_type AS type_of_payment,
session_parent.insurance_provider,


parent.parent_last_login_date,
parent.parent_first_login_date,
session_parent.first_session_frequency,
session_parent.reason_for_visit,
session_parent.payment_type,
session_parent.first_session_date,
session_parent.most_recent_session_date,
session_parent.upcoming_session_date,
session_parent.upcoming_session_time,
session_parent.most_recent_provider_name,
session_parent.upcoming_provider_name,

session_parent.farthest_out_session_date,
session_parent.most_recent_provider_name,
session_parent.upcoming_provider_name,
session_parent.most_recent_appt_type,
session_parent.neverbookedFLG,
session_parent.engagement_status,
session_parent.session_count
FROM {{ref('int__parent')}} AS parent
LEFT JOIN {{ref('int__session_parent')}} AS session_parent
ON parent.parent_id = session_parent.parent_id