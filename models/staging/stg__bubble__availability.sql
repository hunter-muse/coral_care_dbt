with source as (
      select * from {{ source('bubble', 'availability') }}
),
renamed as (
    select
        {{ adapter.quote("AVAILABILITY_ID") }},
        {{ adapter.quote("ACTIVE") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("DURATION") }},
        {{ adapter.quote("END_TIME") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("NUMBER_120") }},
        {{ adapter.quote("NUMBER_30") }},
        {{ adapter.quote("NUMBER_45") }},
        {{ adapter.quote("NUMBER_60") }},
        {{ adapter.quote("NUMBER_75") }},
        {{ adapter.quote("NUMBER_90") }},
        {{ adapter.quote("OFFSET") }},
        {{ adapter.quote("PROVIDER_DETAIL") }},
        {{ adapter.quote("START_TIME") }},
        {{ adapter.quote("TIMEZONE") }},
        {{ adapter.quote("WORKING_DAYS") }}

    from source
)
select * from renamed
  