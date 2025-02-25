-- Check for duplicates by email
WITH email_duplicates AS (
  SELECT 
    --parent_first_name || ' ' || parent_last_name as parent_full_name,
    parent_email as duplicate_record,
    COUNT(*) as record_count,
    LISTAGG(coral_care_status, ', ') as status_list,
    LISTAGG(record_source, ', ') as source_list,
    LISTAGG(record_id, ', ') as record_ids,
    --LISTAGG(parent_full_name, ', ') as full_names,
    MIN(parent_hubspot_created_date) as earliest_create_date,
    MAX(parent_hubspot_created_date) as latest_create_date,
    'Email Duplicate' as duplicate_type
  FROM {{ref('stg__hubspot__contact_parent')}}
  WHERE 
    segment_multi = 'Parent'
    AND parent_email IS NOT NULL
  GROUP BY parent_email
  HAVING COUNT(*) > 1
),

-- Check for duplicates by name
name_duplicates AS (
  SELECT 
    parent_first_name || ' ' || parent_last_name as duplicate_record,
    COUNT(*) as record_count,
    LISTAGG(coral_care_status, ', ') as status_list,
    LISTAGG(record_source, ', ') as source_list,
    LISTAGG(record_id, ', ') as record_ids,
   -- LISTAGG(parent_email, ', ') as email_addresses,
    MIN(parent_hubspot_created_date) as earliest_create_date,
    MAX(parent_hubspot_created_date) as latest_create_date,
    'Name Duplicate' as duplicate_type
  FROM {{ref('stg__hubspot__contact_parent')}}
  WHERE 
    --segment_multi = 'Parent'
     parent_first_name IS NOT NULL
    AND parent_last_name IS NOT NULL
  GROUP BY parent_first_name, parent_last_name
  HAVING COUNT(*) > 1
)

-- Union the results
, final as (
SELECT * FROM email_duplicates
UNION ALL
SELECT * FROM name_duplicates
)

SELECT * FROM final
ORDER BY duplicate_type 