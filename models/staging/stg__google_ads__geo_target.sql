with source as (
        select * from {{ source('google_ads', 'geo_target') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ID") }},
        {{ adapter.quote("PARENT_GEO_TARGET_ID") }},
        {{ adapter.quote("CANONICAL_NAME") }},
        {{ adapter.quote("COUNTRY_CODE") }},
        {{ adapter.quote("NAME") }},
        {{ adapter.quote("STATUS") }},
        {{ adapter.quote("TARGET_TYPE") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }}

      from source
  )
  select * from renamed
    