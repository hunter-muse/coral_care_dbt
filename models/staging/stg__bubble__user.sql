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
      -- State: Get MA from third part
      CASE
        WHEN REGEXP_LIKE(
          SUBSTRING(user.location_address, 1, 1),
          '^[0-9]$'
        ) THEN TRIM(
          REGEXP_SUBSTR(
            SPLIT_PART(user.location_address, ',', 3),
            '[A-Z]{2}'
          )
        )
        ELSE TRIM(
          REGEXP_SUBSTR(
            SPLIT_PART(user.location_address, ',', 2),
            '[A-Z]{2}'
          )
        )
      END AS state,
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
select * from renamed
WHERE 1=1
AND email not ILIKE '%joincoral%'
AND email not ILIKE '%pronoco%'
AND email not ILIKE '%+test%'
AND last_name not ILIKE '%parent%'
AND last_name NOT ILIKE '%test%'