with source as (
      select * from {{ source('bubble', 'temp_busy_slot') }}
),
renamed as (
    select
        {{ adapter.quote("SLOT_ID") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("DELETED_SLOT") }},
        {{ adapter.quote("DURATION_IN_MINUTES") }},
        {{ adapter.quote("END_DATE") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("PROVIDER_DETAIL") }},
        {{ adapter.quote("SESSION") }},
        {{ adapter.quote("START_DATE") }}

    from source
)
select * from renamed
  