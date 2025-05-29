-- snapshots/parent_funnel_snapshot.sql

{% snapshot parent_funnel_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='coral_parent_id',
      strategy='timestamp',
      updated_at='date_time',
      tags=['snapshot', 'weekly'] 
    )
}}

with parent_funnel as (
select distinct 
coral_parent_id,
current_stage, 
hours_in_current_stage, 
ROUND(hours_in_current_stage / 24, 2) as days_in_current_stage,
total_days_in_funnel,
current_timestamp as date_time
from 
{{ref('analytics__parent_funnel')}}
)

select * from parent_funnel

{% endsnapshot %}
