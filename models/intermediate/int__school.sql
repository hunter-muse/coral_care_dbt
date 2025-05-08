-- int__school.sql
with school_data as (
    select distinct 
        -- Create unique coral_school_id using MD5 hash of contact_id
        MD5(contact_id::VARCHAR) as coral_school_id,
        
        -- Contact Information
        contact_id as hubspot_school_id,
        school_contact_first_name,
        school_contact_last_name,
        school_contact_email,
        school_contact_phone,
        school_contact_title,
        
        -- School Information
        school_name,
        school_name_alternate,
        school_company_name,
        school_type,
        school_address,
        elementary_school_name,
        prek_facility_name,
        
        -- Segment Information
        segment,
        segment_multi,
        partner_status,
        active_partner_status,
        
        -- Location Information
        CAST(latitude as FLOAT) as school_location_lat,
        CAST(longitude as FLOAT) as school_location_lng,
        --street_address as school_address,
        city as school_city,
        state as school_state,
        state_region_code as school_state_region_code,
        zip_code as school_postal_code,
        
        -- Student Information
        total_students,
        student_age_ranges,
        
        -- Service Information
        services_interested,
        screening_type,
        evaluation_type,
        
        -- Provider Information
        slp_in_area,
        ot_in_area,
        
        -- Communication & Source Information
        hear_about_us_source,
        utm_campaign,
        utm_source,
        utm_medium,
        utm_content,
        utm_term,
        utm_channel,
        original_traffic_source,
        original_traffic_source_drill_down_1,
        original_traffic_source_drill_down_2,
        
        -- Dates and Activity
        CAST(school_hubspot_created_date as DATE) as school_created_date,
        CAST(last_modified_date as DATE) as school_last_modified_date,
        CAST(last_activity_date as DATE) as school_last_activity_date,
        CAST(last_engagement_date as DATE) as school_last_engagement_date,
        CAST(last_contacted as DATE) as school_last_contacted_date,
        
        -- Email Status
        unsubscribed_from_emails as school_unsubscribed_from_emails
    from {{ ref('stg__hubspot__contact_school') }}
)

select * from school_data