with source as (
        select * from {{ source('facebook_ads', 'delivery_purchase_roas') }}
  ),
  renamed as (
      select
          {{ adapter.quote("AD_ID") }},
        {{ adapter.quote("DATE") }},
        {{ adapter.quote("_FIVETRAN_ID") }},
        {{ adapter.quote("ACCOUNT_ID") }},
        {{ adapter.quote("INLINE_LINK_CLICKS") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }}

      from source
  )
  select * from renamed
    