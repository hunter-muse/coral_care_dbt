with source as (
      select * from {{ source('bubble', 'provider') }}
),
renamed as (
    select
        {{ adapter.quote("PROVIDER_ID") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("SLUG") }},
        {{ adapter.quote("ABOUT") }},
        {{ adapter.quote("ACTIVE") }},
        {{ adapter.quote("ADDRESS") }},
        {{ adapter.quote("ADDRESS_LAT") }},
        {{ adapter.quote("ADDRESS_LNG") }},
        {{ adapter.quote("CERTIFICATION_INFO") }},
        {{ adapter.quote("DEGREE_INFO") }},
        {{ adapter.quote("EXPECTED_HOURS") }},
        {{ adapter.quote("FIRST_LAST") }},
        TRIM(
        SPLIT_PART(TRIM(First_Last), ' ', 1)
      ) AS provider_first_name,
      CASE
        WHEN TRIM(
          SPLIT_PART(TRIM(First_Last), ' ', 2)
        ) = '' THEN TRIM(
          SPLIT_PART(TRIM(First_Last), '  ', 2)
        )
        ELSE TRIM(
          SPLIT_PART(TRIM(First_Last), ' ', 2)
        )
      END AS provider_last_name,
        {{ adapter.quote("HOURLY_RATE") }},
        {{ adapter.quote("IN_NETWORK") }},
        {{ adapter.quote("INSURANCE_LIST") }},
        {{ adapter.quote("LANGUAGES") }},
        {{ adapter.quote("LAST_UPDATE") }},
        {{ adapter.quote("NPI_NUMBER") }},
        {{ adapter.quote("PHONE") }},
        {{ adapter.quote("PRACTICE_STATES") }},
        {{ adapter.quote("PROFILE_PICTURE") }},
        {{ adapter.quote("PROVIDER_SPECIALTY") }},
        {{ adapter.quote("STATUS") }},
        {{ adapter.quote("EDUCATION_LIST") }},
        {{ adapter.quote("JOB_HISTORY_LIST") }},
        {{ adapter.quote("LICENSE_TYPE") }},
        {{ adapter.quote("CASH_PAY") }},
        {{ adapter.quote("INSURANCE_PAY") }},
        {{ adapter.quote("XANO_ID") }},
        {{ adapter.quote("LAST_XANO_SYNC_DATE") }},
        {{ adapter.quote("TIMEZONE") }},
        {{ adapter.quote("TIMEZONE_OFFSET") }},
        {{ adapter.quote("TRAVEL_RADIUS") }},
        {{ adapter.quote("ACCEPT_NEW_PATIENTS") }},
        {{ adapter.quote("TARGET_CASELOAD") }}
    from source
    
)
select * from renamed 
where 
    provider_last_name NOT ILIKE '%test%'
    AND provider_last_name NOT ILIKE '%deprecated%'
    AND provider_last_name NOT ILIKE '%2%'
    AND provider_last_name NOT ILIKE '%DNU%'
    AND (provider_last_name != 'alyssa Broadley' OR provider_last_name IS NULL)  -- Modified to handle NULL cases
    and provider_id != '1726529868906x282251167612796930' --bad data 
    and provider_id != '1737674727112x449477585706418200' --bad data 
    