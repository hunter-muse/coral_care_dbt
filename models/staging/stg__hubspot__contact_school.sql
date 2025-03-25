with zip_matches as (
    -- First try to join on zip code
    select 
        c.ID,
        lat_long_zip.LATITUDE,
        lat_long_zip.LONGITUDE
    from {{source('hubspot', 'contact')}} as c
    left join {{ref('lat_long_zip')}} as lat_long_zip
        on CASE 
            WHEN LENGTH(TRIM(c.PROPERTY_ZIP)) = 4 THEN '0' || c.PROPERTY_ZIP
            ELSE c.PROPERTY_ZIP
        END = CAST(lat_long_zip.ZIP as string)
    where c.PROPERTY_ZIP is not null
)

, city_state_matches as (
    -- For records without a zip match, try to match on city and state
    select 
        c.ID,
        lat_long_zip.LATITUDE,
        lat_long_zip.LONGITUDE
    from {{source('hubspot', 'contact')}} as c
    left join {{ref('lat_long_zip')}} as lat_long_zip
        on c.PROPERTY_CITY = lat_long_zip.CITY_NAME
        and c.PROPERTY_HS_STATE_CODE = lat_long_zip.STATE_ABBV
   -- where c.ID not in (select ID from zip_matches where LATITUDE is not null)
    qualify row_number() over (partition by c.ID order by lat_long_zip.ZIP) = 1
)

select
    -- Basic Information
    contact.ID as contact_id,
    PROPERTY_FIRSTNAME as school_contact_first_name,
    PROPERTY_LASTNAME as school_contact_last_name,
    PROPERTY_EMAIL as school_contact_email,
    PROPERTY_PHONE as school_contact_phone,
    PROPERTY_TITLE as school_contact_title,
    
    -- School Information
    PROPERTY_SCHOOL_NAME as school_name,
    PROPERTY_SCHOOL_NAME_2 as school_name_alternate,
    PROPERTY_COMPANY as school_company_name,
    PROPERTY_SCHOOL_TYPE as school_type,
    PROPERTY_SCHOOL_ADDRESS as school_address,
    PROPERTY_NAME_OF_FACILITY_ELEMENTARY_SCHOOLS as elementary_school_name,
    PROPERTY_NAME_OF_FACILITY_PREK as prek_facility_name,
    
    -- Segment Information
    PROPERTY_SEGMENT as segment,
    PROPERTY_SEGMENT_MULTI as segment_multi,
    PROPERTY_PARTNER_STATUS as partner_status,
    PROPERTY_ACTIVE_PARTNER as active_partner_status,
    
    -- Location Information
    CAST(COALESCE(
        contact.PROPERTY_LATITUDE, 
        zip_matches.LATITUDE,
        city_state_matches.LATITUDE
    ) as string) as latitude,
    CAST(COALESCE(
        contact.PROPERTY_LONGITUDE, 
        zip_matches.LONGITUDE,
        city_state_matches.LONGITUDE
    ) as string) as longitude,
    contact.PROPERTY_ADDRESS as street_address,
    contact.PROPERTY_CITY as city,
    coalesce(contact.PROPERTY_STATE, contact.PROPERTY_HS_STATE_CODE) as state,
    contact.PROPERTY_HS_STATE_CODE as state_region_code,
    CASE 
        WHEN LENGTH(TRIM(contact.PROPERTY_ZIP)) = 4 THEN '0' || contact.PROPERTY_ZIP
        ELSE contact.PROPERTY_ZIP
    END AS zip_code,
    
    -- Student Information
    PROPERTY_WHAT_IS_THE_TOTAL_NUMBER_OF_STUDENTS_ as total_students,
    PROPERTY_WHAT_ARE_THE_AGES_OF_THE_STUDENTS_ as student_age_ranges,
    
    -- Service Information
    PROPERTY_WHAT_CORAL_CARE_SERVICES_ARE_YOU_INTERESTED_IN_ as services_interested,
    PROPERTY_TYPE_OF_SCREENING as screening_type,
    PROPERTY_EVALUATION as evaluation_type,
    
    -- Provider Information
    PROPERTY_SLP_IN_AREA as slp_in_area,
    PROPERTY_OT_IN_AREA as ot_in_area,
    
    -- Communication & Source Information
    PROPERTY_HOW_DID_YOU_HEAR_ABOUT_US_ as hear_about_us_source,
    PROPERTY_UTM_CAMPAIGN as utm_campaign,
    PROPERTY_UTM_SOURCE as utm_source,
    PROPERTY_UTM_MEDIUM as utm_medium,
    PROPERTY_UTM_CONTENT as utm_content,
    PROPERTY_UTM_TERM as utm_term,
    PROPERTY_UTM_CHANNEL as utm_channel,
    PROPERTY_HS_ANALYTICS_SOURCE as original_traffic_source,
    PROPERTY_HS_ANALYTICS_SOURCE_DATA_1 as original_traffic_source_drill_down_1,
    PROPERTY_HS_ANALYTICS_SOURCE_DATA_2 as original_traffic_source_drill_down_2,
    
    -- Dates and Activity
    PROPERTY_CREATEDATE as school_hubspot_created_date,
    PROPERTY_LASTMODIFIEDDATE as last_modified_date,
    PROPERTY_HS_LAST_SALES_ACTIVITY_DATE as last_activity_date,
    PROPERTY_HS_LAST_SALES_ACTIVITY_TIMESTAMP as last_engagement_date,
    PROPERTY_NOTES_LAST_CONTACTED as last_contacted,
    
    -- Email Status
    CASE WHEN PROPERTY_hs_email_optout IS NULL THEN FALSE ELSE PROPERTY_hs_email_optout END AS unsubscribed_from_emails

from {{source('hubspot', 'contact')}} as contact
left join zip_matches 
    on contact.ID = zip_matches.ID
left join city_state_matches
    on contact.ID = city_state_matches.ID
where 
PROPERTY_SEGMENT_MULTI = 'School'
AND 
is_deleted = FALSE