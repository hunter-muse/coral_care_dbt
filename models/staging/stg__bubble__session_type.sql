with source as (
      select * from {{ source('bubble', 'session_type') }}
),
renamed as (
    select
        {{ adapter.quote("SESSION_TYPE_ID") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("DURATION") }},
        {{ adapter.quote("FORMAT") }},
        {{ adapter.quote("INTAKE_FORM_URL") }},
        {{ adapter.quote("LOCATION_TYPE") }},
        {{ adapter.quote("NAME") }},
        {{ adapter.quote("PROVIDER_TYPE") }},
        {{ adapter.quote("SESSION_FORM_URL") }},
        {{ adapter.quote("STATUS") }},
        {{ adapter.quote("RECURRING") }}

    from source
)
select * from renamed
  