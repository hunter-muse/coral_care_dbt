with source as (
        select * from {{ source('google_ads', 'landing_page_stats') }}
  ),
  renamed as (
      select
          {{ adapter.quote("CUSTOMER_ID") }},
        {{ adapter.quote("DATE") }},
        {{ adapter.quote("_FIVETRAN_ID") }},
        {{ adapter.quote("CAMPAIGN_ID") }},
        {{ adapter.quote("IMPRESSIONS") }},
        {{ adapter.quote("AD_GROUP_ID") }},
        {{ adapter.quote("MOBILE_FRIENDLY_CLICKS_PERCENTAGE") }},
        {{ adapter.quote("CLICKS") }},
        {{ adapter.quote("RESOURCE_NAME") }},
        {{ adapter.quote("UNEXPANDED_FINAL_URL") }},
        {{ adapter.quote("SPEED_SCORE") }},
        {{ adapter.quote("AVERAGE_CPC") }},
        {{ adapter.quote("VALID_ACCELERATED_MOBILE_PAGES_CLICKS_PERCENTAGE") }},
        {{ adapter.quote("CTR") }},
        {{ adapter.quote("COST_MICROS") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }}

      from source
  )
  select * from renamed
    