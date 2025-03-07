-- tests/warn_duplicate_parent_emails.sql
{{ config(severity='warn') }}

select 
    parent_email, 
    count(*) as email_count
from {{ ref('dim__parent') }}
where parent_email is not null
group by parent_email
having count(*) > 1