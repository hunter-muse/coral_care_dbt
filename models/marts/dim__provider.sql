with hubspot_funnel_prep as (
  select 
  f.coral_provider_id,
  f.provider_lead_status, 
  MAX(d.credentialing_form) as credentialing_form
  from {{ ref('int__hubspot_provider_funnel') }} f 
  left join {{ref('stg__hubspot__deal')}} d on f.hubspot_provider_id = d.contact_id 
  group by f.coral_provider_id, f.provider_lead_status
)

select distinct 
        provider.coral_provider_id,
      bubble_provider_id,
      bubble_provider_user_id,
      provider.hubspot_provider_id,
      provider_first_name,
      provider_last_name,
      provider_phone_number,
      provider_email,
      provider_address,
      address,
      city,
      state,
      provider.postal_code,
      latitude,
      longitude,
      provider_product_status,
      hubspot_provider_availability_status,
      bubble_provider_availability_status,
      provider_about,
      Active_Flag,
      Certification_Info,
      Degree_Info,
      Estimated_Hours_Per_Week,
      Hourly_Rate,
      IN_NETWORK,
      insurances_accepted_names,
      Spoken_Languages,
      NPI_Number,
      Provider_States_Practicing,
      Travel_Radius,
      Provider_Specialty,
      Education_List,
      License_Type,
      cash_pay_flag,
      insurance_pay_flag,
      Created_Date,
      signup_completed_date,
      last_login_date,
      first_login_date,
      provider_unsubscribed_from_emails,
      hear_about_us_source_provider,
      provider_last_contacted,
      provider_last_engagement_date,
      provider_engagement_notes,
      accept_new_patients,
      target_caseload,
      total_completed_confirmed_sessions,
      last_completed_confirmed_appt,
      next_upcoming_appt,
      first_completed_appointment,
      furthest_upcoming_session,
      single_session_completion_rate,
      recurring_session_completion_rate,
      last_completed_confirmed_appt_format,
      --provider_lifecycle_status AS provider_lifecycle_status_raw,
      CASE WHEN provider_lifecycle_status = 'Lead' THEN hspf.provider_lead_status ELSE provider_lifecycle_status END as provider_lifecycle_status,
      hspf.credentialing_form
      --provider_lead_status 
from {{ ref('int__provider') }} provider
left join hubspot_funnel_prep hspf on provider.coral_provider_id = hspf.coral_provider_id
left join {{ ref('int__provider_scorecard') }} ps on provider.coral_provider_id = ps.coral_provider_id