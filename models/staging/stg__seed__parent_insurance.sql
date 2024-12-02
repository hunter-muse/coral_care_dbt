select 
    PROPERTY_EMAIL as parent_email,
    m.standardized_name as insurance_name
from {{ source('hubspot', 'contact') }} p
left join {{ ref('stg__seed__insurance_mapping') }} m
where p.PROPERTY_INSURANCE_PROVIDER_PROD = m.source_insurance_name
    or array_contains(p.PROPERTY_INSURANCE_PROVIDER_PROD::variant, split(m.parent_aliases, ','))
    and p.PROPERTY_SEGMENT = 'Parent'