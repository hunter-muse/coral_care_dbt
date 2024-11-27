with source as (
      select * from {{ source('bubble', 'session_provider') }}
),
renamed as (
    select
        {{ adapter.quote("PROVIDER_ID") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("DURATION_MINS") }},
        {{ adapter.quote("FORMAT_TYPE") }},
        {{ adapter.quote("LOCATION") }},
        {{ adapter.quote("NAME") }},
        {{ adapter.quote("SESSION_TYPE") }},
        {{ adapter.quote("STATUS") }},
        {{ adapter.quote("PROVIDER_REFERENCE") }}

    from source
)
select * from renamed
  