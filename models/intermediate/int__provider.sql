with provider_user_prep as (
    SELECT  
        -- If they exist in Hubspot, always use that ID, otherwise use Bubble ID
        CASE
            WHEN provider.record_id IS NOT NULL THEN 'HS_' || provider.record_id
            ELSE user.user_id
        END as coral_provider_id,
        COALESCE(user.First_Name, provider.provider_first_name) as provider_first_name,
        COALESCE(user.Last_Name, provider.provider_last_name) as provider_last_name,
        COALESCE(user.Role, 'Provider') as provider_role,
        provider.provider_availability_status,
        CASE 
            WHEN user.user_id IS NOT NULL THEN user.source
            WHEN provider.record_id IS NOT NULL THEN 'hubspot'
            ELSE NULL 
        END as source,
        user.email as provider_email,
        user.signup_completed_date, 
        user.last_login_date,
        user.created_date as first_login_date,
        provider.provider_lifecycle_status,
        provider.record_id as hubspot_provider_id,
        user.user_id as bubble_provider_user_id,
        provider.created_date as provider_created_date
    from {{ref('stg__hubspot__contact_provider')}} provider
    FULL OUTER JOIN {{ref('stg__bubble__user')}} user
        on lower(user.first_name) = lower(provider.provider_first_name)
        and lower(user.last_name) = lower(provider.provider_last_name)
        and user.role = 'Provider'
    WHERE provider.record_id IS NOT NULL  -- Include all Hubspot providers
        OR (user.role = 'Provider')  
    --QUALIFY row_number() over (partition by provider_first_name, provider_last_name order by provider.created_date, provider_first_name, provider_last_name) = 1 
),

provider_user as (
select * from provider_user_prep
qualify row_number() over (partition by provider_first_name, provider_last_name order by provider_created_date, provider_first_name, provider_last_name) = 1 --gets the most recent provider record from Hubspot
),

insurance_mapping as (
    select * from {{ ref('stg__bubble__insurance') }}
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
      CONCAT(
        '+1 (',
        SUBSTRING(provider.Phone, 1, 3),
        ')-',
        SUBSTRING(provider.Phone, 4, 3),
        '-',
        SUBSTRING(provider.Phone, 7, 4)
      ) AS provider_phone_number,
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
provider.Address_Lat AS latitude, 
provider.Address_Lng AS longitude,
--provider.radius,  
provider.Status AS provider_product_status,
provider.About as provider_about, 
provider.active as Active_Flag, 
provider.Certification_Info, 
provider.Degree_Info, 
provider.Expected_Hours, 
provider.Hourly_Rate,
provider.IN_NETWORK, 
provider.Insurance_List, 
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
provider.Provider_Specialty, 
provider.Travel_Radius,
provider.Education_List, 
provider.License_Type, 
provider.cash_pay as Cash_Pay_Flag,
provider.insurance_pay as Insurance_Pay_Flag,
provider.Created_Date,
provider.timezone_offset,
user.signup_completed_date,
user.last_login_date,
user.first_login_date,
user.provider_availability_status,
user.provider_lifecycle_status,
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
      CASE
        WHEN insurance_id = '1726511314167x111587402655465470' THEN 'Cigna'
        WHEN insurance_id = '1710254843190x710132183505829900' THEN 'Mass General Brigham Health Plan'
        WHEN insurance_id = '1706282212033x919311988805075000' THEN 'Harvard Pilgram Healthcare'
        WHEN insurance_id = '1695687925125x330305090873458700' THEN 'Blue Cross Blue Shield of Massachusetts'
        WHEN insurance_id = '1693283593098x737059778370994200' THEN 'Self Pay'
        ELSE 'Unknown Insurance'
      END AS insurance_name
    FROM
      insurance_list
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
      provider_detail.state,
      provider_detail.zip AS postal_code,
      provider_detail.latitude,
      provider_detail.longitude,
      provider_detail.provider_product_status,
      provider_detail.provider_availability_status,
      provider_detail.provider_lifecycle_status,
      provider_detail.provider_about,
      provider_detail.Active_Flag,
      provider_detail.Certification_Info,
      provider_detail.Degree_Info,
      provider_detail.Expected_Hours AS Estimated_Hours_Per_Week,
      provider_detail.Hourly_Rate,
      provider_detail.IN_NETWORK,
      provider_insurance_accepted.insurances_accepted_names,
      provider_detail.Spoken_Languages,
      provider_detail.NPI_Number,
      provider_detail.Provider_States_Practicing,
      provider_detail.Travel_Radius,
      provider_detail.Provider_Specialty,
      provider_detail.Education_List,
      provider_detail.License_Type,
      provider_detail.cash_pay_flag,
      provider_detail.insurance_pay_flag,
      provider_detail.Created_Date,
      provider_detail.signup_completed_date,
      provider_detail.last_login_date,
      provider_detail.first_login_date
    FROM
      provider_detail
    LEFT JOIN
      provider_insurance_accepted ON 
      provider_detail.Provider_First_Name = provider_insurance_accepted.Provider_First_Name 
      AND provider_detail.Provider_Last_Name = provider_insurance_accepted.Provider_Last_Name