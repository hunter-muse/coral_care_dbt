select
    -- Basic Provider Information
    ID as hubspot_provider_id,
    PROPERTY_EMAIL as provider_email,
    concat(PROPERTY_FIRSTNAME, ' ', PROPERTY_LASTNAME) as provider_full_name,
    PROPERTY_FIRSTNAME AS provider_first_name, 
    PROPERTY_LASTNAME as provider_last_name,
    PROPERTY_WHAT_TYPE_OF_PROVIDER_ARE_YOU_ as specialty,
    PROPERTY_PHONE as phone_number,
    PROPERTY_ADDRESS as address,
    PROPERTY_STATE as state,
    PROPERTY_SEGMENT as segment,
    CASE 
        WHEN LENGTH(TRIM(PROPERTY_ZIP)) = 4 THEN '0' || PROPERTY_ZIP
        ELSE PROPERTY_ZIP
    END AS zip_code,
    
    -- Location Information
    CAST(COALESCE(PROPERTY_LATITUDE, lat_long_zip.LATITUDE) as string) as latitude,
    CAST(COALESCE(PROPERTY_LONGITUDE, lat_long_zip.LONGITUDE) as string) as longitude,
    
    -- Dates and Activity
    PROPERTY_CREATEDATE as created_date,
    PROPERTY_HS_LAST_SALES_ACTIVITY_DATE as last_activity_date,
    
    -- Provider Credentials and Details
    PROPERTY_WHAT_IS_YOUR_NPI_NUMBER_ as npi_number,
    PROPERTY_WHAT_AGE_GROUPS_ARE_YOU_QUALIFIED_TO_WORK_WITH_ as qualified_age_groups,
    PROPERTY_WHAT_IS_YOUR_CAQH_ID_ as caqh_id,
    
    -- Status Fields
    /* Note: I don't see exact matches for these fields in HubSpot, 
       but including them in case they're added later or exist under different names */
    NULL as eligible_for_sign_on_bonus,  -- Field not found in HubSpot
    NULL as checkr_status,               -- Field not found in HubSpot
    NULL as docusign_status,             -- Field not found in HubSpot
    
    -- Contract Information
    PROPERTY_CONTRACT_SIGNED_DATE as contract_signed,
    
    -- Provider Details
    PROPERTY_PROVIDER_TYPE_PROD as provider_type,
    PROPERTY_TRAVEL_RADIUS as radius,
    PROPERTY_WHAT_INSURANCE_COMPANIES_ARE_YOU_CREDENTIALED_WITH_ as insurance_accepted,
    
    -- Marketing Information
    PROPERTY_UTM_CAMPAIGN as utm_campaign,
    PROPERTY_HS_ANALYTICS_SOURCE as original_traffic_source,

    -- Specialty Information
    PROPERTY_SPEECH_LANGUAGE_PATHOLOGY_SPECIALTIES as slp_specialties,
    PROPERTY_COMPLEX_CASES_SLP as complex_cases_slp,
    PROPERTY_PEDIATRIC_OCCUPATIONAL_THERAPY_SPECIALTIES as ot_specialties,
    PROPERTY_COMPLEX_CASES as complex_cases_ot,  -- Based on context
    PROPERTY_PHYSICAL_THERAPY_PT_SPECIALTIES as pt_specialties,
    PROPERTY_COMPLEX_CASES_PT as complex_cases_pt,
    PROPERTY_FEEDING_AND_SWALLOWING as sensory_feeding_issues,
    PROPERTY_FEEDING_DISORDERS_RELATED_TO_DYSPHAGIA as feeding_disorders_dysphagia,
    PROPERTY_G_TUBE_FEEDING_ISSUES as g_tube_feeding_issues,
    PROPERTY_MOBILITY_AIDS as mobility_aids,
    PROPERTY_ADAPTIVE_TOOLS as adaptive_tools,
    -- No exact match found for "Adaptive Play"
    PROPERTY_MYOFUNCTIONAL_DISORDERS_ as myofunctional_disorders,
    PROPERTY_LOW_TECH_AAC_SYSTEMS as low_tech_aac,
    PROPERTY_HIGH_TECH_AAC_SYSTEMS as high_tech_aac,

    -- Provider Status and Estimates
    PROPERTY_UNIQUE_PATIENTS_L_30 as unique_patients_l_30,
    PROPERTY_SESSIONS_L_30 as sessions_l_30,
    PROPERTY_PROVIDER_STATUS_DETAIL as provider_availability_status, --provider_status_detail,
    PROPERTY_PROVIDER_STATUS as provider_lifecycle_status, --provider_status,
    PROPERTY_PROVIDER_STATUS_PRODUCT as provider_product,--provider_status_product,
    PROPERTY_ESTIMATE_WEEKLY_HOURS as estimate_weekly_hours,
    PROPERTY_HS_V_2_DATE_ENTERED_LEAD as date_entered_lead,
    PROPERTY_SEGMENT_MULTI,
    PROPERTY_NOTES_LAST_CONTACTED as last_contacted,
    PROPERTY_HS_LAST_SALES_ACTIVITY_TIMESTAMP as last_engagement_date,
    PROPERTY_HEAR_ABOUT_US_PROVIDER_2 as hear_about_us_source_provider, 
    PROPERTY_CREATEDATE AS provider_hubspot_created_date,
    CASE WHEN PROPERTY_hs_email_optout IS NULL THEN FALSE ELSE PROPERTY_hs_email_optout END AS unsubscribed_from_emails,
    -- PROPERTY_SEGMENT

from {{source('hubspot', 'contact')}} AS contact
left join {{ref('lat_long_zip')}} AS lat_long_zip
    on CAST(contact.PROPERTY_ZIP as string) = CAST(lat_long_zip.ZIP as string)
where 
PROPERTY_SEGMENT_MULTI = 'Provider'
AND 
is_deleted = FALSE 
qualify row_number() over (partition by PROPERTY_FIRSTNAME, PROPERTY_LASTNAME order by PROPERTY_CREATEDATE) = 1 --remove duplicates