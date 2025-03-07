-- tests/warn_duplicate_parent_names.sql
{{ config(severity='warn') }}

select 
    parent_first_name, 
    parent_last_name, 
    count(*) as name_count
from {{ ref('dim__parent') }}
where parent_first_name is not null 
  and parent_last_name is not null
  and bubble_parent_id IS NOT NULL 
group by parent_first_name, parent_last_name
having count(*) > 1