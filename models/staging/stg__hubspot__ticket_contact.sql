with source as (
        select * from {{ source('hubspot', 'ticket_contact') }}
  ),
  renamed as (
      select
          {{ adapter.quote("TICKET_ID") }},
        {{ adapter.quote("CATEGORY") }},
        {{ adapter.quote("CONTACT_ID") }},
        {{ adapter.quote("TYPE_ID") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }}

      from source
  )
  select * from renamed
    