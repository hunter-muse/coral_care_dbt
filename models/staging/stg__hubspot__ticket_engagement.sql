with source as (
        select * from {{ source('hubspot', 'ticket_engagement') }}
  ),
  renamed as (
      select
          {{ adapter.quote("TICKET_ID") }},
        {{ adapter.quote("CATEGORY") }},
        {{ adapter.quote("ENGAGEMENT_ID") }},
        {{ adapter.quote("ENGAGEMENT_TYPE") }},
        {{ adapter.quote("TYPE_ID") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }}

      from source
  )
  select * from renamed
    