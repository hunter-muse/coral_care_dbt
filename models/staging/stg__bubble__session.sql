with source as (
      select * from {{ source('bubble', 'session') }}
),
renamed as (
    select
        {{ adapter.quote("SESSION_ID") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("APPOINTMENT") }},
        {{ adapter.quote("DEPENDANT") }},
        {{ adapter.quote("END_DATE") }},
        {{ adapter.quote("PARENT_USER") }},
        {{ adapter.quote("PROVIDER_USER") }},
        {{ adapter.quote("PROVIDER_DETAILS") }},
        {{ adapter.quote("SESSION_TYPE") }},
        {{ adapter.quote("START_DATE") }},
        {{ adapter.quote("STATUS") }},
        {{ adapter.quote("STRIPE_PAYMENT_ID") }},
        {{ adapter.quote("TOTAL_COST") }},
        {{ adapter.quote("ADDRESS_TEXT") }},
        {{ adapter.quote("SESSION_FORM_URL") }},
        {{ adapter.quote("PAYMENT_STATUS") }},
        {{ adapter.quote("PAYMENT_TYPE") }},
        {{ adapter.quote("CHARGE_ID") }},
        {{ adapter.quote("REASON_FOR_VISIT") }},
        {{ adapter.quote("ADDRESS_LAT") }},
        {{ adapter.quote("ADDRESS_LNG") }},
        {{ adapter.quote("ADDRESS_CITY") }},
        {{ adapter.quote("ADDRESS_STATE") }},
        {{ adapter.quote("WAITING_FOR_STRIPE") }},
        {{ adapter.quote("RECURRING_DIFFERENTIATION") }},
        {{ adapter.quote("ORIGINAL_SESSION") }},
        {{ adapter.quote("TEMP_BUSY_SLOT") }},
        {{ adapter.quote("EXPIRATION_DATE") }},
        {{ adapter.quote("PROVIDER_NAME") }},
        {{ adapter.quote("PRETTY_FORMAT") }},
        {{ adapter.quote("SHOW_THANK_YOU") }},
        {{ adapter.quote("LEGACY_WITH_INSURANCE") }},
        {{ adapter.quote("PARENT_DETAIL") }},
        {{ adapter.quote("XANO_ID") }},
        {{ adapter.quote("XANO_LAST_SYNC_DATE") }},
        {{ adapter.quote("DURATION") }},
        {{ adapter.quote("SESSION_FORMAT") }},
        {{ adapter.quote("LATE_CANCELLATION_FEE_WAIVED") }},
        {{ adapter.quote("FIRST_SESSION") }},
        {{ adapter.quote("CC_LAST_4") }}

    from source
)
select * from renamed
  