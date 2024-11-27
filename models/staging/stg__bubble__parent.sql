with source as (
      select * from {{ source('bubble', 'parent') }}
),
renamed as (
    select
        {{ adapter.quote("PARENT_ID") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("DEPENDANT_LIST") }},
        {{ adapter.quote("USER_REFERENCE") }},
        {{ adapter.quote("REQUIRED_SERVICES") }}

    from source
)
select * from renamed
  