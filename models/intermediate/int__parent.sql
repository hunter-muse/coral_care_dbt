with bubble_parents as (
    SELECT user.*, parent.parent_id
    FROM {{ ref('stg__bubble__parent') }} as parent
    LEFT JOIN {{ ref('stg__bubble__user') }} as user
        on parent.user_reference = user.user_id
),   
    
parent_join as (
    SELECT 
    MD5(COALESCE(hubspot.record_id::VARCHAR, '') || COALESCE(bubble.user_id::VARCHAR, '')) as coral_parent_id,
    COALESCE(bubble.first_name, hubspot.parent_first_name) as parent_first_name,
    COALESCE(bubble.last_name, hubspot.parent_last_name) as parent_last_name,
    hubspot.record_id as hubspot_parent_id,
    bubble.user_id as bubble_parent_user_id,
    bubble.parent_id as bubble_parent_id, 
    COALESCE(bubble.email, hubspot.parent_email) as parent_email,
    COALESCE(bubble.Phone, hubspot.parent_phone) as parent_phone,
    bubble.location_address as parent_location_address,
    COALESCE(bubble.address, hubspot.street_address) as parent_address,
    COALESCE(bubble.city, hubspot.city) as parent_city,
    COALESCE(bubble.state, hubspot.state) as parent_state,
    COALESCE(bubble.postal_code, hubspot.zip_code) as parent_postal_code,
    COALESCE(bubble.location_lat, hubspot.latitude) as parent_location_lat,
    COALESCE(bubble.location_lng, hubspot.longitude) as parent_location_lng,
    CASE 
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('self-pay', 'out-of-network') 
            THEN 'Self-Pay (includes out-of-network)'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('blue-cross-blue-shield-of-massachusetts', 'blue cross blue shield (bcbs)', 'blue cross blue shield of massachusetts', 'blue cross blue shield of massachussetts', 'bcbs tx', 'anthem') 
            THEN 'Blue Cross Blue Shield of Massachusetts'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('harvard-pilgrim-healthcare', 'harvard pilgrim healthcare') 
            THEN 'Harvard Pilgrim Healthcare'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('mass-general-brigham-health-plan', 'mass general brigham health plan', 'mass general brigham') 
            THEN 'Mass General Brigham Health Plan'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('cigna') 
            THEN 'Cigna'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('tufts-health-plan', 'tufts health plan') 
            THEN 'Tufts Health Plan'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('mass health', 'medicaid') 
            THEN 'MassHealth'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) = 'wellsense' 
            THEN 'WellSense'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) = 'aetna' 
            THEN 'Aetna'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('united', 'unitedhealth') 
            THEN 'UnitedHealth Care'
        WHEN hubspot.insurance_provider IS NULL OR TRIM(hubspot.insurance_provider) = 'N/A'
            THEN NULL
        ELSE 'Other'
    END as parent_insurance_provider,
    CAST(COALESCE(bubble.last_login_date, hubspot.last_login_date) as date) as parent_last_login_date, 
    hubspot.provider_type AS parent_provider_type_needed, 
    hubspot.unsubscribed_from_emails as parent_unsubscribed_from_emails,
    hubspot.hear_about_us_source_parent, 
    hubspot.parent_re_engage_notes,
    CAST(hubspot.last_contacted as date) as parent_last_contacted,
    CAST(hubspot.last_engagement_date as date) as parent_last_engagement_date,
    CAST(bubble.created_date as date) as parent_first_login_date
    FROM bubble_parents bubble
    FULL OUTER JOIN {{ref('stg__hubspot__contact_parent')}} as hubspot
        on TRIM(lower(bubble.first_name)) = TRIM(lower(hubspot.parent_first_name))
        and TRIM(lower(bubble.last_name)) = TRIM(lower(hubspot.parent_last_name))
)
    
select parent_join.*
from parent_join