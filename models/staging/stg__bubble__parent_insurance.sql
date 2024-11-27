with source as (
      select * from {{ source('bubble', 'parent_insurance') }}
),
renamed as (
    select
        {{ adapter.quote("INSURANCE_ID") }},
        {{ adapter.quote("MODIFIED_DATE") }},
        {{ adapter.quote("CREATED_DATE") }},
        {{ adapter.quote("CREATED_BY") }},
        {{ adapter.quote("INSURANCE_PROVIDER") }},
        {{ adapter.quote("CARD_BACK_IMAGE") }},
        {{ adapter.quote("CARD_FRONT_IMAGE") }},
        {{ adapter.quote("GROUP_NUMBER") }},
        {{ adapter.quote("HEALTH_PLAN") }},
        {{ adapter.quote("MEMBER_ID") }},
        {{ adapter.quote("PARENT_REFERENCE") }},
        {{ adapter.quote("POLICY_HOLDER") }},
        {{ adapter.quote("POLICY_HOLDER_DOB") }},
        {{ adapter.quote("USER_REFERENCE") }},
        {{ adapter.quote("STATUS") }}

    from source
)
select * from renamed
  