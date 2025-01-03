select 
    session_ID, 
    appointment as appointment_id,
    dependant as dependent_id, 
    parent_user as parent_id,
    parent_detail, 
    session_format, 
    duration, 
    --provider_user, 
    provider.provider_id, 
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
    cancellation_reason,
    cancellation_timestamp_date,
    y_notes_completed_timestamp,
    CASE WHEN DATEDIFF(HOUR, start_date, cancellation_timestamp_date) > -24 AND DATEDIFF(HOUR, start_date, cancellation_timestamp_date) < 0 THEN 1 ELSE 0 END as is_cancellation_before_24_hours,
    CASE WHEN DATEDIFF(HOUR, start_date, cancellation_timestamp_date) > 0 THEN 1 ELSE 0 END AS Post_session_start_cancellation,
    DATEDIFF(HOUR, start_date, cancellation_timestamp_date) as hours_before_cancellation,
    CASE 
        when user.role = 'Parent' then 'Parent'
        when user.role = 'Provider' then 'Provider'
        when user.role = 'App admin' then 'Admin'
    END as session_created_by_role,
    --TIME(DATEADD(MINUTE, -provider.timezone_offset, start_date)) as session_start_time_only,
    --provider.timezone_offset as timezone_offset
from {{ ref('stg__bubble__session') }} as session
left join {{ref('int__provider')}} as provider 
    on session.provider_user = provider.provider_id
left join {{ref('stg__bubble__user')}} as user
    on session.created_by = user.user_id