with source as (
        select * from {{ source('google_ads', 'user_list') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ID") }},
        {{ adapter.quote("ACCESS_REASON") }},
        {{ adapter.quote("ACCOUNT_USER_LIST_STATUS") }},
        {{ adapter.quote("CLOSING_REASON") }},
        {{ adapter.quote("DESCRIPTION") }},
        {{ adapter.quote("ELIGIBLE_FOR_DISPLAY") }},
        {{ adapter.quote("ELIGIBLE_FOR_SEARCH") }},
        {{ adapter.quote("MEMBERSHIP_STATUS") }},
        {{ adapter.quote("NAME") }},
        {{ adapter.quote("READ_ONLY") }},
        {{ adapter.quote("SIZE_FOR_DISPLAY") }},
        {{ adapter.quote("SIZE_FOR_SEARCH") }},
        {{ adapter.quote("SIZE_RANGE_FOR_DISPLAY") }},
        {{ adapter.quote("SIZE_RANGE_FOR_SEARCH") }},
        {{ adapter.quote("TYPE") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }}

      from source
  )
  select * from renamed
    