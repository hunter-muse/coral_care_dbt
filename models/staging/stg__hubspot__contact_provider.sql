select
    -- Basic Provider Information
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
    PROPERTY_WHAT_AGE_GROUPS_ARE_YOU_QUALIFIED_TO_WORK_WITH_ as qualified_age_groups,
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
    PROPERTY_HIGH_TECH_AAC_SYSTEMS as high_tech_aac

from {{source('hubspot', 'contact')}} AS contact
left join {{ref('lat_long_zip')}} AS lat_long_zip
    on CAST(contact.PROPERTY_ZIP as string) = CAST(lat_long_zip.ZIP as string)
where PROPERTY_SEGMENT = 'Provider'