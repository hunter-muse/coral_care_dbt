with source as (
        select * from {{ source('google_ads', 'account_hourly_stats') }}
  ),
  renamed as (
      select
          {{ adapter.quote("CUSTOMER_ID") }},
        {{ adapter.quote("DATE") }},
        {{ adapter.quote("_FIVETRAN_ID") }},
        {{ adapter.quote("CLICK_TYPE") }},
        {{ adapter.quote("CONVERSIONS") }},
        {{ adapter.quote("MONTH") }},
        {{ adapter.quote("INTERACTIONS") }},
        {{ adapter.quote("YEAR") }},
        {{ adapter.quote("AVERAGE_CPM") }},
        {{ adapter.quote("INTERACTION_EVENT_TYPES") }},
        {{ adapter.quote("DEVICE") }},
        {{ adapter.quote("WEEK") }},
        {{ adapter.quote("ACTIVE_VIEW_IMPRESSIONS") }},
        {{ adapter.quote("CLICKS") }},
        {{ adapter.quote("DAY_OF_WEEK") }},
        {{ adapter.quote("ACTIVE_VIEW_MEASURABLE_IMPRESSIONS") }},
        {{ adapter.quote("QUARTER") }},
        {{ adapter.quote("COST_PER_CONVERSION") }},
        {{ adapter.quote("ACTIVE_VIEW_MEASURABILITY") }},
        {{ adapter.quote("AVERAGE_CPC") }},
        {{ adapter.quote("CTR") }},
        {{ adapter.quote("CONVERSIONS_VALUE") }},
        {{ adapter.quote("AVERAGE_COST") }},
        {{ adapter.quote("AD_NETWORK_TYPE") }},
        {{ adapter.quote("INTERACTION_RATE") }},
        {{ adapter.quote("IMPRESSIONS") }},
        {{ adapter.quote("ACTIVE_VIEW_VIEWABILITY") }},
        {{ adapter.quote("VALUE_PER_CONVERSION") }},
        {{ adapter.quote("HOUR") }},
        {{ adapter.quote("ACTIVE_VIEW_CPM") }},
        {{ adapter.quote("ACTIVE_VIEW_CTR") }},
        {{ adapter.quote("ACTIVE_VIEW_MEASURABLE_COST_MICROS") }},
        {{ adapter.quote("CONVERSIONS_FROM_INTERACTIONS_RATE") }},
        {{ adapter.quote("COST_MICROS") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }}

      from source
  )
  select * from renamed
    