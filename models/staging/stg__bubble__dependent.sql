with source as (
      select * from {{ source('bubble', 'dependent') }}
),
renamed as (
    select
        {{ adapter.quote("DEPENDENT_ID") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("CREATED_BY") }},
        CAST({{ adapter.quote("DOB") }} as date) as dob,
        {{ adapter.quote("GENDER") }},
        {{ adapter.quote("DEPENDENT_NAME") }},
        {{ adapter.quote("SHARE_DETAILS") }},
        {{ adapter.quote("PARENT_ID") }}
        --{{ adapter.quote("PROVIDER_ID_LIST") }}

    from source
)
select * from renamed
where dependent_name NOT ILIKE 'test%'
and dependent_name NOT ILIKE '%test'
and dependent_name NOT ILIKE 'kiddo%'
and dependent_name NOT ILIKE 'fakechild%'
and dependent_name NOT ILIKE '%deprecated%'
and dependent_name NOT ILIKE '%snowperson%'
  