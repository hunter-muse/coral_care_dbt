with source as (
        select * from {{ source('google_ads', 'responsive_search_ad_history') }}
  ),
  renamed as (
      select
          {{ adapter.quote("AD_GROUP_ID") }},
        {{ adapter.quote("AD_ID") }},
        {{ adapter.quote("UPDATED_AT") }},
        {{ adapter.quote("DESCRIPTIONS") }},
        {{ adapter.quote("HEADLINES") }},
        {{ adapter.quote("PATH_1") }},
        {{ adapter.quote("PATH_2") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }},
        {{ adapter.quote("_FIVETRAN_START") }},
        {{ adapter.quote("_FIVETRAN_END") }},
        {{ adapter.quote("_FIVETRAN_ACTIVE") }}

      from source
  )
  select * from renamed
    