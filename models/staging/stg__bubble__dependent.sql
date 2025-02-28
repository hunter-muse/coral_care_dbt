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
  