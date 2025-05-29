-- snapshots/provider_funnel_snapshot.sql

{% snapshot provider_funnel_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='coral_provider_id',
      strategy='timestamp',
      updated_at='date_time',
      tags=['snapshot', 'weekly'] 
    )
}}

with provider_funnel as (
select distinct 
coral_provider_id,
recruiting_current_stage, 
hours_in_current_stage, 
ROUND(hours_in_current_stage / 24, 2) as days_in_current_stage,
total_days_in_funnel,
current_timestamp as date_time
from 
{{ref('analytics__provider_funnel')}}
)

select * from provider_funnel

{% endsnapshot %}
