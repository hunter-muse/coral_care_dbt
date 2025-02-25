with source as (
      select * from {{ source('bubble', 'provider_tag') }}
),
renamed as (
    select
        {{ adapter.quote("TAG_ID") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("SLUG") }},
        {{ adapter.quote("ACTIVE") }},
        {{ adapter.quote("NAME") }},
        {{ adapter.quote("XANO_ID") }}

    from source
)
select * from renamed
  