with source as (
      select * from {{ source('bubble', 'user') }}
),
renamed as (
    select
        {{ adapter.quote("USER_ID") }},
        {{ adapter.quote("IS_DEVELOPER") }},
        {{ adapter.quote("FIRST_NAME") }},
        {{ adapter.quote("LAST_NAME") }},
        {{ adapter.quote("ROLE") }},
        {{ adapter.quote("SIGNUP_COMPLETED_DATE") }},
        {{ adapter.quote("SIGNUP_STEP") }},
        {{ adapter.quote("LOGIN_METHOD") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("SLUG") }},
        {{ adapter.quote("PICTURE") }},
        {{ adapter.quote("LAST_LOGIN_DATE") }},
        {{ adapter.quote("LANGUAGE") }},
        {{ adapter.quote("TIME_ZONE") }},
        {{ adapter.quote("LOCATION_ADDRESS") }},
        {{ adapter.quote("LOCATION_LAT") }},
        {{ adapter.quote("LOCATION_LNG") }},
        {{ adapter.quote("PHONE") }},
        {{ adapter.quote("EMAIL_NOTIFICATIONS") }},
        {{ adapter.quote("SOURCE") }},
        {{ adapter.quote("TIME_ZONE_OFFSET") }},
        {{ adapter.quote("HAS_PASSWORD") }},
        {{ adapter.quote("EMAIL") }}

    from source
)
select * from renamed
  