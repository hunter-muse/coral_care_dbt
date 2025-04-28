-- snapshots/provider_metrics_snapshot.sql

{% snapshot provider_metrics_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='provider_metrics_id',
      strategy='timestamp',
      updated_at='date_time',
      tags=['snapshot', 'weekly'] 
    )
}}

with time_to_x_sessions as (
  Select 
pro.provider_first_name || ' ' || pro.provider_last_name as provider_name
,pro.coral_provider_id
,coalesce(hp.DATE_ENTERED_LEGACY_ONBOARDING_COMPLETE_PROVIDER_ONBOARDING,FIRST_LOGIN_DATE) as onboarding_date
,session_created_date
,datediff(DAY,
coalesce(hp.DATE_ENTERED_LEGACY_ONBOARDING_COMPLETE_PROVIDER_ONBOARDING,FIRST_LOGIN_DATE)
,session_created_date) as time_to_session
,rank() over (partition by pro.coral_provider_id order by session_created_date asc) as session_id_rank2

from {{ref('fact__session')}} sess
join {{ref('dim__provider')}} pro on sess.coral_provider_id = pro.coral_provider_id
left join {{ref('analytics__provider_funnel')}} hp on pro.coral_provider_id = hp.coral_provider_id
where session_status IN ('Completed','Confirmed')
),

final_data as (

select 
current_date as date_time,  -- Move this up to define date_time first
concat(provider.coral_provider_id, '-', current_date) as provider_metrics_id,  -- Use current_date directly
provider.provider_first_name || ' ' || provider.provider_last_name as provider_name,
provider.coral_provider_id,
coalesce(provider.state, hp.state) as state,
case when coalesce(provider.provider_product_status, hp.provider_product_status) = 'Suspended' then 'Suspended_'||coalesce(provider.provider_specialty, hp.provider_specialty) else coalesce(provider.provider_specialty, hp.provider_specialty) end as provider_specialty,
coalesce(provider.INSURANCES_ACCEPTED_NAMES, hp.insurances_accepted_names) as insurances_accepted_names,
provider.accept_new_patients,
provider.target_caseload,
coalesce(provider.travel_radius, hp.travel_radius) as travel_radius,
case when coalesce(provider.latitude, hp.latitude, zip.latitude) < 0 then coalesce(provider.longitude, hp.longitude, zip.longitude) else coalesce(provider.latitude, hp.latitude, zip.latitude) end as latitude,
case when coalesce(provider.longitude, hp.longitude, zip.longitude) > 0 then coalesce(provider.latitude, hp.latitude, zip.latitude) else coalesce(provider.longitude, hp.longitude, zip.longitude) end as longitude,
-- Rest of your columns same as before
coalesce(hp.completed_last_30_days,0) as l_30_day_sessions,
coalesce(hp.unique_patient_count_last_30_days,0) as caseload_l_30_days,
hp.total_completed_session_count,
coalesce(provider.provider_product_status, hp.provider_product_status) as provider_product_status,
provider.hubspot_provider_availability_status,
coalesce(provider.bubble_provider_availability_status , hp.bubble_provider_availability_status) as bubble_provider_availability_status,
coalesce(provider.PROVIDER_LIFECYCLE_STATUS, hp.provider_lifecycle_status) as provider_lifecycle_status,
provider.provider_about,
provider.PROVIDER_ENGAGEMENT_NOTES,
coalesce(provider.estimated_hours_per_week, hp.estimated_hours_per_week) as estimated_hours_per_week,
spec.TOTAL_SPECIALITIES,
spec.infants,
spec.toddlers,
spec.preschoolers,
spec.school_aged_children,
spec.adolescents,
net_avail.monday_potential_hours as monday_availability,
net_avail.TUESDAY_potential_hours as tuesday_availability,
net_avail.WEDNESDAY_potential_hours as wednesday_availability,
net_avail.THURSDAY_potential_hours as thursday_availability,
net_avail.FRIDAY_potential_hours as friday_availability,
net_avail.SATURDAY_potential_hours as saturday_availability,
net_avail.SUNDAY_potential_hours as sunday_availability,
net_avail.TOTAL_WEEKLY_NET_AVAILABLE_HOURS as total_weekly_available_hours,
net_avail.weekly_morning_net_hours as morning_weekly_available_hours,
net_avail.weekly_afternoon_net_hours as afternoon_weekly_available_hours,
net_avail.weekly_evening_net_hours as evening_weekly_available_hours,
coalesce(pf.RECRUITING_CURRENT_STAGE, SALES_CURRENT_STAGE) as current_stage,
pf.CLOSED_LOST_PROVIDER_RECRUITING_STATUS,
coalesce(RECRUITING_TOTAL_DAYS_IN_FUNNEL,0)+coalesce(SALES_TOTAL_DAYS_IN_FUNNEL,0) as total_days_in_funnel,
pf.DATE_ENTERED_NEW_PROVIDER_LEAD_PROVIDER_RECRUITING::Date as DATE_ENTERED_NEW_PROVIDER_LEAD_PROVIDER_RECRUITING,
pf.DATE_ENTERED_CLINICAL_INTERVIEW_PROVIDER_RECRUITING::Date as DATE_ENTERED_CLINICAL_INTERVIEW_PROVIDER_RECRUITING,
pf.DATE_ENTERED_OFFER_LETTER_PROVIDER_RECRUITING::Date as DATE_ENTERED_OFFER_LETTER_PROVIDER_RECRUITING,
pf.DATE_ENTERED_LEGACY_PENDING_ONBOARDING_TASKS_PROVIDER_ONBOARDING::Date as DATE_ENTERED_PENDING_ONBOARDING_TASKS_PROVIDER_ONBOARDING,
pf.DATE_ENTERED_LEGACY_ONBOARDING_COMPLETE_PROVIDER_ONBOARDING::Date as DATE_ENTERED_ONBOARDING_COMPLETE_PROVIDER_ONBOARDING,
DAYS_IN_NEW_PROVIDER_LEAD_PROVIDER_RECRUITING,
DAYS_IN_CLINICAL_INTERVIEW_PROVIDER_RECRUITING,
DAYS_IN_OFFER_LETTER_PROVIDER_RECRUITING,
coalesce(DAYS_IN_LEGACY_PENDING_ONBOARDING_TASKS_PROVIDER_ONBOARDING,DAYS_IN_PENDING_TASKS_PROVIDER_RECRUITING) as days_in_onboarding_tasks,
coalesce(DAYS_IN_LEGACY_SCHEDULE_ONBOARDING_CALL_PROVIDER_ONBOARDING,DAYS_IN_ONBOARDING_CALL_PROVIDER_RECRUITING) as days_in_onboarding_call,
coalesce(FARTHEST_OUT_SESSION_DATE,hp.most_recent_session_date) as last_session,
CERTIFICATION_INFO,
ttx.time_to_session as time_to_1st_booked_session,
ttx2.time_to_session as time_to_2nd_booked_session,
ttx3.time_to_session as time_to_5th_booked_session,
ttx4.time_to_session as time_to_10th_booked_session,
ttx5.time_to_session as time_to_25th_booked_session,
coalesce(RECRUITING_HOURS_IN_CURRENT_STAGE,SALES_HOURS_IN_CURRENT_STAGE)/24.0 as days_in_current_stage,
case when coalesce(provider.INSURANCES_ACCEPTED_NAMES, hp.insurances_accepted_names) is null then 0
    else coalesce(length(coalesce(provider.INSURANCES_ACCEPTED_NAMES, hp.insurances_accepted_names))- length(replace(coalesce(provider.INSURANCES_ACCEPTED_NAMES, hp.insurances_accepted_names),',','')),0)+1 end as total_insurances_enrolled,
rank() over (partition by provider.provider_first_name || ' ' || provider.provider_last_name, provider.state, provider.provider_specialty order by random()) as deal_id_rank


from 
{{ref('dim__provider')}} provider 
left join {{ref('dim__provider_subspecialty')}} spec on provider.coral_provider_id = spec.coral_provider_id
left join {{ref('dim__provider_availability')}} avail on provider.coral_provider_id = avail.coral_provider_id
left join {{ref('mart__hubspot_provider')}} hp on provider.coral_provider_id = hp.coral_provider_id
left join {{ref('analytics__provider_funnel')}} pf on provider.coral_provider_id = pf.coral_provider_id
left join {{ref('dim__provider_weekly_availability')}} net_avail on provider.coral_provider_id = net_avail.coral_provider_id and net_avail.time_period = 'Current Week'
left join time_to_x_sessions ttx on provider.coral_provider_id = ttx.coral_provider_id and ttx.session_id_rank2 = 1 
left join time_to_x_sessions ttx2 on provider.coral_provider_id = ttx2.coral_provider_id and ttx2.session_id_rank2 = 2 
left join time_to_x_sessions ttx3 on provider.coral_provider_id = ttx3.coral_provider_id and ttx3.session_id_rank2 = 5 
left join time_to_x_sessions ttx4 on provider.coral_provider_id = ttx4.coral_provider_id and ttx4.session_id_rank2 = 10 
left join time_to_x_sessions ttx5 on provider.coral_provider_id = ttx5.coral_provider_id and ttx5.session_id_rank2 = 25 
left join {{ref('lat_long_zip')}} zip on provider.postal_code = zip.zip_code 

where (provider.provider_lifecycle_status is not null OR provider.provider_product_status = 'Approved')
and provider.provider_email not like '%@joincoralcare.com'
and coalesce(provider.provider_product_status, hp.provider_product_status) is not null

order by 2,2,1)

Select * from final_data 
where deal_id_rank = 1

{% endsnapshot %}