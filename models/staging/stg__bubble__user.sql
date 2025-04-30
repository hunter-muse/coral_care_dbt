with source as (
      select * from {{ source('bubble', 'user') }}
),
renamed as (
    select
        {{ adapter.quote("USER_ID") }},
        {{ adapter.quote("IS_DEVELOPER") }},
        TRIM({{ adapter.quote("FIRST_NAME") }}) as {{ adapter.quote("FIRST_NAME") }},
        TRIM({{ adapter.quote("LAST_NAME") }}) as {{ adapter.quote("LAST_NAME") }},
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
        CASE
        WHEN REGEXP_LIKE(
          SUBSTRING(user.location_address, 1, 1),
          '^[0-9]$'
        ) THEN TRIM(
          SPLIT_PART(user.location_address, ',', 1)
        )
        ELSE NULL
      END AS address,
      -- City: Second part (after first comma, before second comma)
      CASE
        WHEN REGEXP_LIKE(
          SUBSTRING(user.location_address, 1, 1),
          '^[0-9]$'
        ) THEN TRIM(
          SPLIT_PART(user.location_address, ',', 2)
        )
        ELSE TRIM(
          SPLIT_PART(user.location_address, ',', 1)
        )
      END AS city,
      -- State: Improved extraction with pattern matching for state codes
      {{ standardize_state(
        'CASE
          -- Check if the input contains a valid state code followed by a space and numbers (like "MA 02090")
          WHEN REGEXP_LIKE(
            TRIM(SPLIT_PART(user.location_address, \',\', 2)),
            \'^[A-Z]{2} [0-9]\'
          ) THEN SUBSTRING(TRIM(SPLIT_PART(user.location_address, \',\', 2)), 1, 2)
          
          -- Check if the input contains a valid state code followed by a space and numbers in the 3rd part
          WHEN REGEXP_LIKE(
            TRIM(SPLIT_PART(user.location_address, \',\', 3)),
            \'^[A-Z]{2} [0-9]\'
          ) THEN SUBSTRING(TRIM(SPLIT_PART(user.location_address, \',\', 3)), 1, 2)
          
          -- Standard pattern matching for state codes
          WHEN REGEXP_LIKE(SUBSTRING(user.location_address, 1, 1), \'^[0-9]$\') THEN TRIM(
            REGEXP_SUBSTR(SPLIT_PART(user.location_address, \',\', 3), \'[A-Z]{2}\')
          )
          ELSE TRIM(
            REGEXP_SUBSTR(SPLIT_PART(user.location_address, \',\', 2), \'[A-Z]{2}\')
          )
        END'
      ) }} AS state,
      CASE 
          WHEN location_address like '%MA%' THEN 'MA' 
          WHEN location_address like 'Massachusetts%' THEN 'MA'
          WHEN location_address like '%Texas%' THEN 'TX'
          WHEN location_address like '%NH%' THEN 'NH'
          WHEN location_address like '%RI%' THEN 'RI'
          WHEN location_address like '%TX%' THEN 'TX'
          WHEN location_address like 'California,%' THEN 'CA'
          ELSE NULL 
      END AS state_Sub, 
      CASE
        WHEN REGEXP_LIKE(
          SUBSTRING(user.location_address, 1, 1),
          '^[0-9]$'
        ) THEN TRIM(
          REGEXP_SUBSTR(
            SPLIT_PART(user.location_address, ',', 3),
            '[0-9]{5}'
          )
        )
        ELSE TRIM(
          REGEXP_SUBSTR(
            SPLIT_PART(user.location_address, ',', 2),
            '[0-9]{5}'
          )
        )
      END AS postal_code,
        {{ adapter.quote("LOCATION_LAT") }},
        {{ adapter.quote("LOCATION_LNG") }},
        -- {{ adapter.quote("PHONE") }},
        CONCAT(
        '+1 (',
        SUBSTRING(user.phone, 1, 3),
        ')-',
        SUBSTRING(user.phone, 4, 3),
        '-',
        SUBSTRING(user.phone, 7, 4)
      ) AS Phone,
        {{ adapter.quote("EMAIL_NOTIFICATIONS") }},
        {{ adapter.quote("SOURCE") }},
        {{ adapter.quote("TIME_ZONE_OFFSET") }},
        {{ adapter.quote("HAS_PASSWORD") }},
        {{ adapter.quote("EMAIL") }}

    from source as user 
)

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
    address,
    city,
    COALESCE(state, state_Sub) as state,
    state_Sub,
    postal_code,
    {{ adapter.quote("LOCATION_LAT") }},
    {{ adapter.quote("LOCATION_LNG") }},
    Phone,
    {{ adapter.quote("EMAIL_NOTIFICATIONS") }},
    {{ adapter.quote("SOURCE") }},
    {{ adapter.quote("TIME_ZONE_OFFSET") }},
    {{ adapter.quote("HAS_PASSWORD") }},
    {{ adapter.quote("EMAIL") }}
from renamed
WHERE 1=1
AND email not ILIKE '%joincoral%'
AND email not ILIKE '%pronoco%'
AND email not ILIKE '%+test%'
AND last_name not ILIKE '%parent%'
AND last_name NOT ILIKE '%test%'
and user_id != '1726529461200x698266999552610300' --bad data 
AND user_id != '1705096552006x688511058530192400' --bad data