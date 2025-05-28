with bubble_parents as (
    SELECT user.*, parent.parent_id
    FROM {{ ref('stg__bubble__parent') }} as parent
    LEFT JOIN {{ ref('stg__bubble__user') }} as user
        on parent.user_reference = user.user_id
),   
    
parent_join as (
    SELECT 
    MD5(COALESCE(hubspot.record_id::VARCHAR, '') || COALESCE(bubble.user_id::VARCHAR, '' || COALESCE(bubble.parent_id::VARCHAR, ''))) as coral_parent_id,
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
    COALESCE(hubspot.state,bubble.state) as parent_state,
    COALESCE(hubspot.zip_code, bubble.postal_code ) as parent_postal_code,
    COALESCE(hubspot.latitude, bubble.location_lat) as parent_location_lat,
    COALESCE(hubspot.longitude, bubble.location_lng) as parent_location_lng,
    hubspot.original_traffic_source,
    hubspot.utm_campaign, 
    hubspot.parent_google_click_id,
    CASE 
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('self-pay', 'out-of-network') 
            THEN 'Self-Pay (includes out-of-network)'
        WHEN LOWER(hubspot.type_of_payment) IN ('I prefer to pay myself', 'No, I will pay out of pocket') 
            THEN 'Self-Pay (includes out-of-network)'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('blue-cross-blue-shield-of-massachusetts', 'blue cross blue shield of massachusetts', 'blue cross blue shield of massachussetts', 'anthem') 
            THEN 'Blue Cross Blue Shield of Massachusetts'
        -- WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('blue cross blue shield (bcbs)')
        --     THEN 'Blue Cross Blue Shield (BCBS)'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('bcbs tx', 'bcbs texas', 'blue cross blue sheild of texas', 'blue-cross-blue-shield-tx')
            THEN 'Blue Cross Blue Shield of Texas'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('blue cross blue shield (bcbs)')
            AND COALESCE(hubspot.state,bubble.state) = 'MA' 
            THEN 'Blue Cross Blue Shield of Massachusetts'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('blue cross blue shield (bcbs)')
            AND COALESCE(hubspot.state,bubble.state) = 'TX' 
            THEN 'Blue Cross Blue Shield of Texas'
        WHEN LOWER(TRIM(hubspot.insurance_provider)) IN ('blue cross blue shield (bcbs)')
            THEN 'Blue Cross Blue Shield (BCBS)'
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
    hubspot.dependent_1_needs,
    hubspot.dependent_2_needs,
    hubspot.dependent_3_needs,
    CAST(hubspot.last_contacted as date) as parent_last_contacted,
    CAST(hubspot.last_engagement_date as date) as parent_last_engagement_date,
    CAST(bubble.created_date as date) as parent_first_login_date,
    hubspot.re_matching_pipeline,
    hubspot.re_match_complete,
    hubspot.parent_sunday_availability,
    hubspot.parent_monday_availability,
    hubspot.parent_tuesday_availability,
    hubspot.parent_wednesday_availability,
    hubspot.parent_thursday_availability,
    hubspot.parent_friday_availability,
    hubspot.parent_saturday_availability,
    hubspot.discharge_reason,
    hubspot.discharge_flag
    FROM bubble_parents bubble
    FULL OUTER JOIN {{ref('stg__hubspot__contact_parent')}} as hubspot
        on TRIM(lower(bubble.first_name)) = TRIM(lower(hubspot.parent_first_name))
        and TRIM(lower(bubble.last_name)) = TRIM(lower(hubspot.parent_last_name))
),

-- First collect all ad clicks with campaign info
ads_with_campaigns as (
    select 
        ads.gclid,
        ch.name as campaign_name,
        -- Use row_number to pick one campaign when a click might be associated with multiple
        row_number() over(partition by ads.gclid order by ads.gclid) as rn
    from {{ref('stg__google_ads__click_stats')}} as ads
    left join {{ref('stg__google_ads__campaign_history')}} as ch
        on ads.campaign_id = ch.base_campaign_id
    where ads.gclid is not null
)

-- Now join with parent data, only using one campaign record per click
select distinct 
    pj.*,
    ac.campaign_name
from parent_join pj 
left join ads_with_campaigns ac
    on pj.parent_google_click_id = ac.gclid
    and ac.rn = 1  -- Only take the first campaign record for each click