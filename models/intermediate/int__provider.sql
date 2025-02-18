with provider_user_prep as (
    SELECT  
        MD5(COALESCE(hubspot.hubspot_provider_id::VARCHAR, '') || COALESCE(bubble.user_id::VARCHAR, '')) as coral_provider_id,
        COALESCE(bubble.first_name, hubspot.provider_first_name) as provider_first_name,
        COALESCE(bubble.last_name, hubspot.provider_last_name) as provider_last_name,
        COALESCE(bubble.role, 'Provider') as provider_role,
        hubspot.provider_availability_status,
        CASE 
            WHEN bubble.user_id IS NOT NULL THEN bubble.source
            WHEN hubspot.hubspot_provider_id IS NOT NULL THEN 'hubspot'
            ELSE NULL 
        END as source,
        COALESCE(hubspot.provider_email, bubble.email) as provider_email,
        hubspot.phone_number,
        hubspot.state,
        hubspot.radius,
        hubspot.latitude,
        hubspot.longitude,
        hubspot.insurance_accepted,
        bubble.signup_completed_date, 
        bubble.last_login_date,
        bubble.created_date as first_login_date,
        hubspot.provider_lifecycle_status,
        hubspot.Specialty as hubspot_provider_specialty,
        bubble.user_id as bubble_provider_user_id,
        hubspot.hubspot_provider_id,
        hubspot.unsubscribed_from_emails as provider_unsubscribed_from_emails,
        hubspot.hear_about_us_source_provider, 
        CAST(hubspot.last_contacted as date) as provider_last_contacted,
        CAST(hubspot.last_engagement_date as date) as provider_last_engagement_date,
        hubspot.created_date as provider_created_date
    from {{ref('stg__hubspot__contact_provider')}} hubspot
    FULL OUTER JOIN {{ref('stg__bubble__user')}} bubble
        on lower(bubble.first_name) = lower(hubspot.provider_first_name)
        and lower(bubble.last_name) = lower(hubspot.provider_last_name)
        and bubble.role = 'Provider'
    WHERE 
    hubspot.hubspot_provider_id IS NOT NULL  -- Include all Hubspot providers
        OR (bubble.role = 'Provider')  
),

provider_user as (
select * from provider_user_prep
qualify row_number() over (partition by provider_first_name, provider_last_name order by provider_created_date, provider_first_name, provider_last_name) = 1 --gets the most recent provider record from Hubspot
),

insurance_mapping as (
    select insurance_id, insurance_name from {{ ref('stg__bubble__insurance') }}
),

provider_detail as (
select 
user.coral_provider_id, 
bubble_provider_user_id,
hubspot_provider_id,
provider.provider_id as bubble_provider_id,
--provider.First_Last AS provider_first_last, 
coalesce(provider.provider_first_name, user.provider_first_name) as provider_first_name, 
coalesce(provider.provider_last_name, user.provider_last_name) as provider_last_name, 
coalesce(user.phone_number, CONCAT(
        '+1 (',
        SUBSTRING(provider.Phone, 1, 3),
        ')-',
        SUBSTRING(provider.Phone, 4, 3),
        '-',
        SUBSTRING(provider.Phone, 7, 4)
      )) AS provider_phone_number,
user.provider_email as provider_email,
provider.Address as provider_address, 
CASE
        WHEN REGEXP_LIKE(SUBSTRING(provider.Address, 1, 1), '^[0-9]$') THEN TRIM(SPLIT_PART(provider.Address, ',', 1))
        ELSE NULL
      END AS address,
      -- City: Second part (after first comma, before second comma)
      CASE
        WHEN REGEXP_LIKE(SUBSTRING(provider.Address, 1, 1), '^[0-9]$') THEN TRIM(SPLIT_PART(provider.Address, ',', 2))
        ELSE TRIM(SPLIT_PART(provider.Address, ',', 1))
      END AS city,
      -- State: Get MA from third part
      CASE
        WHEN REGEXP_LIKE(SUBSTRING(provider.Address, 1, 1), '^[0-9]$') THEN TRIM(
          REGEXP_SUBSTR(SPLIT_PART(provider.Address, ',', 3), '[A-Z]{2}')
        )
        ELSE TRIM(
          REGEXP_SUBSTR(SPLIT_PART(provider.Address, ',', 2), '[A-Z]{2}')
        )
      END AS state,
      -- Zip: Get 5-digit number from the appropriate part
      CASE
        WHEN REGEXP_LIKE(SUBSTRING(provider.Address, 1, 1), '^[0-9]$') THEN TRIM(
          REGEXP_SUBSTR(SPLIT_PART(provider.Address, ',', 3), '[0-9]{5}')
        )
        ELSE TRIM(
          REGEXP_SUBSTR(SPLIT_PART(provider.Address, ',', 2), '[0-9]{5}')
        )
      END AS zip,
coalesce(user.latitude, provider.Address_Lat) AS latitude, 
coalesce(user.longitude, provider.Address_Lng) AS longitude,
user.state as hubspot_state,
provider.Status AS provider_product_status,
provider.About as provider_about, 
provider.active as Active_Flag, 
provider.Certification_Info, 
provider.Degree_Info, 
provider.Expected_Hours, 
provider.Hourly_Rate,
provider.IN_NETWORK, 
provider.Insurance_List, 
user.insurance_accepted,
REPLACE(
    REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    provider.Languages,
                    '[',
                    ''
                ),
                ']',
                ''
            ),
            '"',
            ''
        ),
        '''',  -- Remove single quotes
        ''
    ),
    ', ',     -- Replace ", " with "," to clean up spacing
    ','
) AS Spoken_Languages,
provider.NPI_Number, 
REPLACE(
    REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    provider.Practice_States,
                    '[',
                    ''
                ),
                ']',
                ''
            ),
            '"',
            ''
        ),
        '''',  -- Remove single quotes
        ''
    ),
    ', ',     -- Replace ", " with "," to clean up spacing
    ','
) AS Provider_States_Practicing,
COALESCE(provider.Provider_Specialty, user.hubspot_provider_specialty) AS Provider_Specialty, 
COALESCE(provider.Travel_Radius, user.radius ) AS Travel_Radius,
provider.Education_List, 
provider.License_Type, 
provider.cash_pay as Cash_Pay_Flag,
provider.insurance_pay as Insurance_Pay_Flag,
COALESCE(user.provider_created_date,provider.Created_Date) as created_date, 
provider.timezone_offset,
user.signup_completed_date,
user.last_login_date,
user.first_login_date,
user.provider_availability_status,
user.provider_lifecycle_status,
user.provider_unsubscribed_from_emails,
user.provider_last_contacted,
user.hear_about_us_source_provider,
user.provider_last_engagement_date,
from provider_user as user
left join {{ ref('stg__bubble__provider') }} as provider 
    ON lower(user.provider_first_name) = lower(provider.provider_first_name)
    AND lower(user.provider_last_name) = lower(provider.provider_last_name)
qualify row_number() over (
    partition by user.provider_first_name, user.provider_last_name 
    order by provider.Created_Date DESC NULLS LAST
) = 1
),

insurance_list AS (
    SELECT
      Provider_First_Name,
      Provider_Last_Name,
      f.value::string AS insurance_id
    FROM
      provider_detail,
      LATERAL FLATTEN(input => PARSE_JSON(Insurance_List)) f
),

mapped_insurances AS (
    SELECT
      Provider_First_Name,
      Provider_Last_Name,
      CASE WHEN insurance_mapping.insurance_name is null then 'Unknown Insurance' else insurance_mapping.insurance_name end as insurance_name
    FROM
      insurance_list
      LEFT JOIN insurance_mapping ON insurance_list.insurance_id = insurance_mapping.insurance_id
  ),

  provider_insurance_accepted AS (
    SELECT
      Provider_First_Name,
      Provider_Last_Name,
      LISTAGG(insurance_name, ', ') WITHIN GROUP (
        ORDER BY
          insurance_name
      ) AS insurances_accepted_names
    FROM
      mapped_insurances
    GROUP BY
      Provider_First_Name,
      Provider_Last_Name
  ),

  provider_availability as ( --only get providers with active availability
    select 
    distinct provider_detail as bubble_provider_id
    from {{ ref('stg__bubble__availability') }} 
    where 
    active = true 
    and bubble_provider_id is not null
  )

    SELECT
      provider_detail.coral_provider_id,
      provider_detail.bubble_provider_id,
      provider_detail.bubble_provider_user_id,
      provider_detail.hubspot_provider_id,
      provider_detail.provider_first_name,
      provider_detail.provider_last_name,
      provider_detail.provider_phone_number,
      provider_detail.provider_email,
      provider_detail.provider_address,
      provider_detail.address,
      provider_detail.city,
      coalesce(hubspot_state, provider_detail.state) as state,
      provider_detail.zip AS postal_code,
      provider_detail.latitude,
      provider_detail.longitude,
      provider_detail.provider_product_status,
      provider_detail.provider_availability_status AS hubspot_provider_availability_status,
      case when provider_availability.bubble_provider_id is not null then true else false end as bubble_provider_availability_status,
      provider_detail.provider_lifecycle_status,
      provider_detail.provider_about,
      provider_detail.Active_Flag,
      provider_detail.Certification_Info,
      provider_detail.Degree_Info,
      provider_detail.Expected_Hours AS Estimated_Hours_Per_Week,
      provider_detail.Hourly_Rate,
      provider_detail.IN_NETWORK,
      coalesce(insurance_accepted, provider_insurance_accepted.insurances_accepted_names) as insurances_accepted_names,
      provider_detail.Spoken_Languages,
      provider_detail.NPI_Number,
      provider_detail.Provider_States_Practicing,
      provider_detail.Travel_Radius,
      provider_detail.Provider_Specialty,
      provider_detail.Education_List,
      provider_detail.License_Type,
      provider_detail.cash_pay_flag,
      provider_detail.insurance_pay_flag,
      provider_detail.created_date,
      provider_detail.signup_completed_date,
      provider_detail.last_login_date,
      provider_detail.first_login_date,
      provider_detail.provider_unsubscribed_from_emails,
      provider_detail.hear_about_us_source_provider,
      provider_detail.provider_last_contacted,
      provider_detail.provider_last_engagement_date
    FROM
      provider_detail
    LEFT JOIN
      provider_insurance_accepted ON 
      provider_detail.Provider_First_Name = provider_insurance_accepted.Provider_First_Name 
      AND provider_detail.Provider_Last_Name = provider_insurance_accepted.Provider_Last_Name
    LEFT JOIN
      provider_availability ON provider_detail.bubble_provider_id = provider_availability.bubble_provider_id