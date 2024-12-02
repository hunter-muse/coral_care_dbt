-- models/staging/stg_provider_insurances.sql
with split_insurances as (
    select 
        PROPERTY_EMAIL AS provider_email, 
        trim(value) as insurance_name
    from {{ source('hubspot', 'contact') }},
        table(flatten(split(PROPERTY_WHAT_INSURANCE_COMPANIES_ARE_YOU_CREDENTIALED_WITH_, ',')))
    where PROPERTY_SEGMENT = 'Provider'
),
standardized as (
    select 
        s.provider_email,
        m.standardized_name as insurance_name
    from split_insurances s
    left join {{ ref('stg__seed__insurance_mapping') }} m
    where s.insurance_name = m.source_insurance_name
        or array_contains(s.insurance_name::variant, split(m.provider_aliases, ','))
)
select * from standardized