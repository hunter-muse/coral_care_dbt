with source as (
      select * from {{ source('bubble', 'insurance') }}
),
renamed as (
    select
        {{ adapter.quote("INSURANCE_ID") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("ONBOARDING_LABEL") }},
        {{ adapter.quote("LAST_XANO_SYNC_DATE") }},
        {{ adapter.quote("XANO_ID_TEXT") }},
        {{ adapter.quote("CASH_PAY__BOOLEAN") }},
        {{ adapter.quote("INSURANCE_STATUS") }},
        {{ adapter.quote("INSURANCE_NAME") }},
        {{ adapter.quote("SLUG") }},
        {{ adapter.quote("CREATED_AT") }},
        {{ adapter.quote("UPDATED_AT") }}

    from source
)
select * from renamed
  