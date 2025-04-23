with source as (
        select * from {{ source('facebook_ads', 'action_video_view_type') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ACCOUNT_ID") }},
        {{ adapter.quote("DATE") }},
        {{ adapter.quote("_FIVETRAN_ID") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }}

      from source
  )
  select * from renamed
    