{% snapshot provider_availability_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='provider_availability_id',
      strategy='timestamp',
      updated_at='date_time',
      tags=['snapshot','daily'] 
    )
}}

select
current_date as date_time 
, provider.coral_provider_id
, provider_status
, concat(provider.coral_provider_id, '-', current_date) as provider_availability_id,
--, coalesce(provider.bubble_provider_availability_status , hp.bubble_provider_availability_status) as bubble_provider_availability_status
, coalesce(provider.PROVIDER_LIFECYCLE_STATUS, hp.provider_lifecycle_status) as provider_lifecycle_status
, provider.accept_new_patients
, coalesce(provider.INSURANCES_ACCEPTED_NAMES, hp.insurances_accepted_names) as insurances_accepted_names
, provider.target_caseload
, coalesce(provider.travel_radius, hp.travel_radius) as travel_radius
, avail.monday_total_hours
, avail.tuesday_total_hours
, avail.wednesday_total_hours
, avail.thursday_total_hours
, avail.friday_total_hours
, avail.saturday_total_hours
, avail.sunday_total_hours
, avail.weekly_total_hours
, avail.weekly_morning_hours
, avail.weekly_afternoon_hours
, avail.weekly_evening_hours
, net_avail.TOTAL_WEEKLY_NET_AVAILABLE_HOURS as total_NET_weekly_available_hours
from  {{ref('dim__provider')}} provider
left join {{ref('dim__provider_subspecialty')}} spec on provider.coral_provider_id = spec.coral_provider_id
left join {{ref('dim__provider_availability')}} avail on provider.coral_provider_id = avail.coral_provider_id
left join {{ref('mart__hubspot_provider')}} hp on provider.coral_provider_id = hp.coral_provider_id
left join {{ref('analytics__provider_funnel')}} pf on provider.coral_provider_id = pf.coral_provider_id
left join {{ref('dim__provider_weekly_availability')}} net_avail on provider.coral_provider_id = net_avail.coral_provider_id and net_avail.time_period = 'Current Week'
where hp.provider_lifecycle_status <> 'Rejected'
and provider.provider_email not like '%@joincoralcare.com'

{% endsnapshot %}