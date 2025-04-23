with source as (
        select * from {{ source('facebook_ads', 'delivery_platform_actions') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ACCOUNT_ID") }},
        {{ adapter.quote("DATE") }},
        {{ adapter.quote("_FIVETRAN_ID") }},
        {{ adapter.quote("INDEX") }},
        {{ adapter.quote("ACTION_TYPE") }},
        {{ adapter.quote("VALUE") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }},
        {{ adapter.quote("INLINE") }},
        {{ adapter.quote("_7_D_CLICK") }},
        {{ adapter.quote("_1_D_VIEW") }}

      from source
  )
  select * from renamed
    