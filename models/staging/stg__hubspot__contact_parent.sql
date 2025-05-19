WITH deals AS (
  SELECT
    contact_id,
    deal_id,
    deal_name,
    deal_created_date,
    -- Example: assign a simple score to help prioritize
    CASE 
      WHEN deal_name IS NOT NULL AND DATE_ENTERED_OPPORTUNITIES_UNENGAGED IS NOT NULL THEN 2
      WHEN deal_name IS NOT NULL THEN 1
      ELSE 0
    END AS deal_quality_score
  FROM {{ ref('stg__hubspot__deal') }}
),

contacts AS (
  SELECT *
  FROM {{ source('hubspot', 'contact') }}
  where PROPERTY_SEGMENT_MULTI = 'Parent'
),

rankedcontacts AS (
  SELECT 
    contact.*,
    deal.deal_id,
    deal.deal_created_date,
    deal.deal_quality_score,
    ROW_NUMBER() OVER (
      PARTITION BY contact.property_firstname, contact.property_lastname, contact.property_phone
      ORDER BY 
        deal.deal_quality_score DESC,
        deal.deal_created_date DESC
    ) AS rn
  FROM contacts AS contact
  LEFT JOIN deals AS deal
    ON contact.id = deal.contact_id
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
   {{ standardize_state('coalesce(contact.PROPERTY_HS_STATE_CODE, contact.PROPERTY_STATE, contact.PROPERTY_STATE_DROPDOWN)') }} as state,
    CASE 
        WHEN LENGTH(TRIM(contact.PROPERTY_ZIP)) = 4 THEN '0' || contact.PROPERTY_ZIP
        ELSE contact.PROPERTY_ZIP
    END AS zip_code,
    property_hs_analytics_source as parent_original_traffic_source,
    property_hs_google_click_id as parent_google_click_id,
    property_utm_campaign as parent_utm_campaign,
    property_utm_source as parent_utm_source,
    property_utm_medium as parent_utm_medium,
    property_utm_term as parent_utm_term,
    property_utm_content as parent_utm_content,
    property_hs_analytics_last_referrer as parent_page_referrer,
    
    -- Provider & Appointment Information
    PROPERTY_PROVIDER_TYPE_PROD as provider_type,
    PROPERTY_APPOINTMENT_FREQUENCY as appointment_frequency,
    PROPERTY_APPOINTMENT_TYPE as appointment_type,
    PROPERTY_SESSION_COUNT as session_count,
    PROPERTY_inactive_reason as discharge_reason, 
    property_discharged_ AS discharge_flag, 
    property_re_matching_pipeline AS re_matching_pipeline,
    property_re_match_complete AS re_match_complete,

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
and id not in ( --see below for list of duplicate records names
'102707373324',
'89700610309',
'100926803544',
'77017798461',
'74586986304',
'76765581124',
'73051586619',
'25434945455',
'95610061893',
'46812940662',
'5451',
'98673739612',
'54712899488',
'48980972679',
'9983881860',
'101016853424', --Emma Roy - likely NOT a duplicate, but we'll remove her to be safe
'95262303411',
'122684836734'   
)  
and rn = 1


/*
Removing the below duplicate contact records: 
Lauren	Bailey
Carline	Creary
Adriana	Cuddy
Dasha	Curran
Christine	Fahey
Alissa	Karabelas
Javaria	Mahmood
Katie	Manley
Wendy	McKean
Liz	O'Connell
Anna	Pawelczyk
Leandra	Pfleger
Lynnet	Ruiz
Necole	Stephen
Ruby	Tapia
Necole Stephen 
Emma Roy - likely NOT a duplicate, but we'll remove her to be safe
Wendy McKean  
*/