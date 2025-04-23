with source as (
        select * from {{ source('facebook_ads', 'delivery_device') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ACCOUNT_ID") }},
        {{ adapter.quote("DATE") }},
        {{ adapter.quote("_FIVETRAN_ID") }},
        {{ adapter.quote("DEVICE_PLATFORM") }},
        {{ adapter.quote("IMPRESSIONS") }},
        {{ adapter.quote("INLINE_LINK_CLICKS") }},
        {{ adapter.quote("REACH") }},
        {{ adapter.quote("COST_PER_INLINE_LINK_CLICK") }},
        {{ adapter.quote("CPC") }},
        {{ adapter.quote("CPM") }},
        {{ adapter.quote("CTR") }},
        {{ adapter.quote("FREQUENCY") }},
        {{ adapter.quote("SPEND") }},
        {{ adapter.quote("INLINE_LINK_CLICK_CTR") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }}

      from source
  )
  select * from renamed
    