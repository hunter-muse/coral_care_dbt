select 
    session_ID, 
    appointment as appointment_id,
    dependant as dependent_id, 
    parent_user as parent_id,
    parent_detail, 
    session_format, 
    duration, 
    provider_user as provider_id, 
    provider_name,
    provider_specialty AS provider_type,
    session_type as session_type_ID, 
    status as session_status,
    payment_status, 
    payment_type, 
    total_cost as session_total_cost,
    reason_for_visit, 
    address_lat, 
    address_lng, 
    Address_city, 
    Address_state, 
    recurring_differentiation, 
    session_frequency,
    start_date AS session_start_date,
    DATE(start_date) as session_start_date_only,
    -- Adjust the time by subtracting the timezone offset minutes
    TIME(start_date) as session_start_time_only,
    --TIME(DATEADD(MINUTE, -provider.timezone_offset, start_date)) as session_start_time_only,
    provider.timezone_offset as timezone_offset
from {{ ref('stg__bubble__session') }} as session
left join {{ref('int__provider')}} as provider 
    on session.provider_user = provider.user_provider_ID