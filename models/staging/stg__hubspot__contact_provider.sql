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
    PROPERTY_ZIP as postal_code,
    
    -- Location Information
    PROPERTY_LATITUDE as latitude,
    PROPERTY_LONGITUDE as longitude,
    
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
    
    -- Marketing Information
    PROPERTY_UTM_CAMPAIGN as utm_campaign

from {{source('hubspot', 'contact')}}
where PROPERTY_SEGMENT = 'Provider'