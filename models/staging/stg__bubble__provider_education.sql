with source as (
      select * from {{ source('bubble', 'provider_education') }}
),
renamed as (
    select
        {{ adapter.quote("EDUCATION_ID") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("LOCATION") }},
        {{ adapter.quote("PROVIDER") }},
        {{ adapter.quote("QUALIFICATION") }},
        {{ adapter.quote("ATTACHED_TO_PROVIDER") }},
        {{ adapter.quote("XANO_ID") }},
        {{ adapter.quote("LAST_XANO_SYNC_DATE") }}

    from source
)
select * from renamed
  