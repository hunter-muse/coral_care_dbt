with source as (
        select * from {{ source('facebook_ads', 'action_video_view_type_video_p_75_watched_actions') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ACCOUNT_ID") }},
        {{ adapter.quote("DATE") }},
        {{ adapter.quote("_FIVETRAN_ID") }},
        {{ adapter.quote("INDEX") }},
        {{ adapter.quote("ACTION_VIDEO_TYPE") }},
        {{ adapter.quote("VALUE") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }},
        {{ adapter.quote("ACTION_TYPE") }},
        {{ adapter.quote("INLINE") }}

      from source
  )
  select * from renamed
    