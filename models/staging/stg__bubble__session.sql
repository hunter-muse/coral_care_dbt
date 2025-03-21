with source as (
      select * from {{ source('bubble', 'session') }}
),
renamed as (
    select
        {{ adapter.quote("SESSION_ID") }},
        CAST({{ adapter.quote("CREATED_DATE") }} AS TIMESTAMP) AS created_date,
        {{ adapter.quote("CREATED_BY") }},
        CAST({{ adapter.quote("MODIFIED_DATE") }} AS TIMESTAMP) AS modified_date,
        {{ adapter.quote("APPOINTMENT") }},
        {{ adapter.quote("DEPENDANT") }},
        CAST({{ adapter.quote("END_DATE") }} AS TIMESTAMP) AS end_date,
        {{ adapter.quote("PARENT_USER") }},
        {{ adapter.quote("PROVIDER_USER") }},
        {{ adapter.quote("PROVIDER_DETAILS") }},
        {{ adapter.quote("SESSION_TYPE") }},
        CAST({{ adapter.quote("START_DATE") }} AS TIMESTAMP) AS start_date,
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
        CAST({{ adapter.quote("XANO_LAST_SYNC_DATE")}} AS TIMESTAMP) AS xano_last_sync_date,
        {{ adapter.quote("DURATION") }},
        {{ adapter.quote("SESSION_FORMAT") }},
        {{ adapter.quote("LATE_CANCELLATION_FEE_WAIVED") }},
        {{ adapter.quote("FIRST_SESSION") }},
        {{ adapter.quote("CC_LAST_4") }},
        {{ adapter.quote("SESSION_FREQUENCY") }},
        {{ adapter.quote("CANCELLATION_REASON")}},
        {{ adapter.quote("SESSION_PARENT_INSURANCE")}} AS session_parent_insurance_id, 
        CAST({{ adapter.quote("CANCELLATION_TIMESTAMP_DATE")}} AS TIMESTAMP) AS cancellation_timestamp_date,
        CAST({{ adapter.quote("Y_NOTES_COMPLETED_TIMESTAMP")}} AS TIMESTAMP) AS y_notes_completed_timestamp
    from source
)
select * from renamed
  