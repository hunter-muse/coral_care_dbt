select
    source_insurance_name,
    standardized_name,
    provider_aliases,
    parent_aliases
from {{ ref('insurance_mapping') }}