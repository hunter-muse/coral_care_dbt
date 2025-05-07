{% test session_id_quality_test(model) %}

with null_counts as (
    select count(*) as null_count
    from {{ model }}
    where session_ID is null
),

duplicate_counts as (
    select count(*) as duplicate_count
    from (
        select session_ID
        from {{ model }}
        group by session_ID
        having count(*) > 1
    )
)

select *
from null_counts, duplicate_counts
where null_count > 5
   or duplicate_count > 5

{% endtest %} 