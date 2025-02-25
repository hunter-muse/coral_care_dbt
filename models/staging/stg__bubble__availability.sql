with source as (
      select * from {{ source('bubble', 'availability') }}
),
renamed as (
    select
        {{ adapter.quote("AVAILABILITY_ID") }},
        {{ adapter.quote("ACTIVE") }},
        {{ adapter.quote("CREATED_BY") }},
        CAST({{ adapter.quote("CREATED_DATE") }} AS TIMESTAMP) AS created_date,
        {{ adapter.quote("DURATION") }},
        CAST({{ adapter.quote("END_TIME") }} AS TIMESTAMP) AS end_time,
        CAST({{ adapter.quote("MODIFIED_DATE") }} AS TIMESTAMP) AS modified_date,
        {{ adapter.quote("NUMBER_120") }},
        {{ adapter.quote("NUMBER_30") }},
        {{ adapter.quote("NUMBER_45") }},
        {{ adapter.quote("NUMBER_60") }},
        {{ adapter.quote("NUMBER_75") }},
        {{ adapter.quote("NUMBER_90") }},
        {{ adapter.quote("OFFSET") }},
        {{ adapter.quote("PROVIDER_DETAIL") }},
        CAST({{ adapter.quote("START_TIME") }} AS TIMESTAMP) AS start_time,
        {{ adapter.quote("TIMEZONE") }},
        {{ adapter.quote("WORKING_DAYS") }}

    from source
)
select * from renamed
  