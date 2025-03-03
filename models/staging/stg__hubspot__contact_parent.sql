WITH RankedContacts AS (
  SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY PROPERTY_FIRSTNAME, PROPERTY_LASTNAME, PROPERTY_PHONE ORDER BY PROPERTY_CREATEDATE ASC) as rn
  FROM {{source('hubspot', 'contact')}} AS contact
  LEFT JOIN {{ref('lat_long_zip')}} AS lat_long_zip
    ON CAST(contact.PROPERTY_ZIP as string) = CAST(lat_long_zip.ZIP as string)
  WHERE 1=1  
    AND PROPERTY_SEGMENT_MULTI = 'Parent'
    AND is_deleted = FALSE
    
)
    select
    -- Basic Information
    ID as record_id,
    PROPERTY_FIRSTNAME as parent_first_name,
    PROPERTY_LASTNAME as parent_last_name,
    PROPERTY_EMAIL as parent_email,
    PROPERTY_PHONE as parent_phone,
    
    -- Segment Information
    PROPERTY_SEGMENT as segment,
    PROPERTY_SEGMENT_MULTI as segment_multi,
    PROPERTY_CORAL_CARE_STATUS as coral_care_status,
    
    -- Location Information
    CAST(COALESCE(contact.PROPERTY_LATITUDE, lat_long_zip.LATITUDE) as string) as latitude,
    CAST(COALESCE(contact.PROPERTY_LONGITUDE, lat_long_zip.LONGITUDE) as string) as longitude,
    contact.PROPERTY_ADDRESS as street_address,
    contact.PROPERTY_CITY as city,
    COALESCE(contact.PROPERTY_HS_STATE_CODE, contact.PROPERTY_STATE) as state_region_code,
    CASE 
        WHEN LENGTH(TRIM(contact.PROPERTY_ZIP)) = 4 THEN '0' || contact.PROPERTY_ZIP
        ELSE contact.PROPERTY_ZIP
    END AS zip_code,
    
    -- Provider & Appointment Information
    PROPERTY_PROVIDER_TYPE_PROD as provider_type,
    PROPERTY_APPOINTMENT_FREQUENCY as appointment_frequency,
    PROPERTY_APPOINTMENT_TYPE as appointment_type,
    PROPERTY_SESSION_COUNT as session_count,
    
    -- Dependent 1 Information
    PROPERTY_DEPENDENT_FIRST_NAME as dependent_1_first_name,
    PROPERTY_DEPENDENT_LAST_NAME as dependent_1_last_name,
    PROPERTY_DEPENDENT_DOB as dependent_1_dob,
    PROPERTY_DEPENDENT_GENDER as dependent_1_gender,
    PROPERTY_AGE_NUMBER as dependent_1_age_number,
    PROPERTY_AGE_UNIT as dependent_1_age_unit,
    PROPERTY_PLEASE_TELL_US_MORE_ABOUT_YOUR_CHILD_S_NEEDS as dependent_1_needs,
    
    -- Dependent 2 Information
    PROPERTY_DEPENDENT_2_FIRST_NAME as dependent_2_first_name,
    PROPERTY_DEPENDENT_2_LAST_NAME as dependent_2_last_name,
    PROPERTY_DEPENDENT_2_DOB as dependent_2_dob,
    PROPERTY_DEPENDENT_2_GENDER as dependent_2_gender,
    PROPERTY_AGE_NUMBER_DEPENDENT_2 as dependent_2_age_number,
    PROPERTY_AGE_UNIT_DEPENDENT_2 as dependent_2_age_unit,
    PROPERTY_PLEASE_TELL_US_MORE_ABOUT_YOUR_CHILD_S_NEEDS as dependent_2_needs,

    
    -- Dependent 3 Information
    PROPERTY_DEPENDENT_3_FIRST_NAME as dependent_3_first_name,
    PROPERTY_DEPENDENT_3_LAST_NAME as dependent_3_last_name,
    PROPERTY_DEPENDENT_3_DOB as dependent_3_dob,
    PROPERTY_DEPENDENT_3_GENDER_CLONED_ as dependent_3_gender,
    PROPERTY_AGE_NUMBER_DEPENDENT_3 as dependent_3_age_number,
    PROPERTY_AGE_UNIT_DEPENDENT_3 as dependent_3_age_unit,
    PROPERTY_CHILD_3_PLEASE_TELL_US_MORE_ABOUT_YOUR_CHILD_S_NEEDS as dependent_3_needs,
    
    -- Payment & Insurance Information
    PROPERTY_TYPE_OF_PAYMENT_PROD as type_of_payment,
    COALESCE(PROPERTY_WHO_IS_YOUR_HEALTH_INSURANCE_PROVIDER_, PROPERTY_INSURANCE_PROVIDER_PROD) as insurance_provider,
    
    -- Login & Appointment Dates
    PROPERTY_LAST_LOGIN_DATE as last_login_date,
    PROPERTY_FIRST_LOGIN_DATE as first_login_date,
    PROPERTY_NEXT_APPOINTMENT_DATE as next_appointment_date,
    PROPERTY_MOST_RECENT_APPOINTMENT_DATE as most_recent_appointment_date,
    
    -- Days of Week Availability
    PROPERTY_SUNDAY as sunday,
    PROPERTY_MONDAY as monday,
    PROPERTY_TUESDAY as tuesday,
    PROPERTY_WEDNESDAY as wednesday,
    PROPERTY_THURSDAY as thursday,
    PROPERTY_FRIDAY as friday,
    PROPERTY_SATURDAY as saturday,
    
    -- Referral & Source Information
    PROPERTY_NUM_ASSOCIATED_DEALS as referral_count,
    PROPERTY_HS_OBJECT_SOURCE as record_source,
    PROPERTY_HS_OBJECT_SOURCE_DETAIL_1 as record_source_detail_1,
    PROPERTY_HS_LATEST_SOURCE_DATA_1 as record_source_detail_2,
    PROPERTY_HS_LATEST_SOURCE_DATA_2 as record_source_detail_3,
    PROPERTY_HS_ANALYTICS_SOURCE as original_traffic_source,
    PROPERTY_HS_ANALYTICS_SOURCE_DATA_1 as original_traffic_source_drill_down_1,
    PROPERTY_HS_ANALYTICS_SOURCE_DATA_2 as original_traffic_source_drill_down_2,
    PROPERTY_UTM_CAMPAIGN as utm_campaign,
    PROPERTY_UTM_CHANNEL as utm_channel,
    PROPERTY_UTM_SOURCE as utm_source,
    PROPERTY_UTM_MEDIUM as utm_medium,
    PROPERTY_UTM_TERM as utm_term,
    PROPERTY_UTM_CONTENT as utm_content,
    PROPERTY_HS_ANALYTICS_LAST_REFERRER as page_referrer,
    PROPERTY_PAGE_SEARCH_SEGMENT as page_search,
    PROPERTY_HS_ANALYTICS_LAST_URL as referrer_url,
    PROPERTY_PRODUCT_FORM_ID as product_form_id,
    
    -- Activity & Engagement Dates
    PROPERTY_HS_LAST_SALES_ACTIVITY_DATE as last_activity_date,
    PROPERTY_NOTES_LAST_CONTACTED as last_contacted,
    PROPERTY_HS_LAST_SALES_ACTIVITY_TIMESTAMP as last_engagement_date,
    PROPERTY_LASTMODIFIEDDATE as last_modified_date,
    PROPERTY_HS_FEEDBACK_LAST_SURVEY_DATE as last_nps_survey_date,
    PROPERTY_HS_FEEDBACK_LAST_NPS_RATING as last_nps_survey_rating,
    PROPERTY_NUM_ASSOCIATED_DEALS as number_of_associated_deals,
    PROPERTY_HS_V_2_DATE_ENTERED_LEAD as date_entered_lead,
    PROPERTY_HOW_DID_YOU_HEAR_ABOUT_US_ AS hear_about_us_source_parent, 
    PROPERTY_RE_ENGAGE_NOTES as parent_re_engage_notes,
    CASE WHEN PROPERTY_hs_email_optout IS NULL THEN FALSE ELSE PROPERTY_hs_email_optout END AS unsubscribed_from_emails,
    PROPERTY_SUNDAY as parent_sunday_availability,
    PROPERTY_MONDAY as parent_monday_availability,
    PROPERTY_TUESDAY as parent_tuesday_availability,
    PROPERTY_WEDNESDAY as parent_wednesday_availability,
    PROPERTY_THURSDAY as parent_thursday_availability,
    PROPERTY_FRIDAY as parent_friday_availability,
    PROPERTY_SATURDAY as parent_saturday_availability,
    PROPERTY_CREATEDATE AS parent_hubspot_created_date

from RankedContacts AS contact
left join {{ref('lat_long_zip')}} AS lat_long_zip
    on CAST(contact.PROPERTY_ZIP as string) = CAST(lat_long_zip.ZIP as string)
where 
is_deleted = FALSE 
and rn = 1


