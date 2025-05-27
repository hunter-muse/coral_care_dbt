with source as (
        select * from {{ source('hubspot', 'engagement') }}
  ),
  renamed as (
      select
          {{ adapter.quote("ID") }},
        {{ adapter.quote("TYPE") }},
        {{ adapter.quote("PORTAL_ID") }},
        {{ adapter.quote("_FIVETRAN_SYNCED") }}

      from source
  )
  select * from renamed
    