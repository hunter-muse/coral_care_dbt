with claims_with_id as (
    select claims.*, 
    dependent.dependent_id
    from 
    {{ ref('stg__claims__monthly_claims_upload') }} claims 
    LEFT JOIN 
    {{ ref('int__dependent')}} dependent 
    on claims.dependent_first_name = dependent.dependent_first_name
    and claims.dependent_last_name = dependent.dependent_last_name
), 

dependent_aggregated as (
    select 
        dependent_first_name,
        dependent_last_name,
        LISTAGG(DISTINCT dependent_id, ', ') WITHIN GROUP (ORDER BY dependent_id) as dependent_ids
    from claims_with_id
    group by dependent_first_name, dependent_last_name
),

parent_aggregated as (
    select 
        dependent_first_name,
        dependent_last_name,
        LISTAGG(DISTINCT parent_first_name, ', ') WITHIN GROUP (ORDER BY parent_first_name) as parent_first_names,
        LISTAGG(DISTINCT parent_last_name, ', ') WITHIN GROUP (ORDER BY parent_last_name) as parent_last_names
    from {{ ref('int__parent_dependent_crosswalk') }}
    group by dependent_first_name, dependent_last_name
),

provider_prep as (
    select distinct 
        dependent_id, 
        dependent_first_name,
        dependent_last_name,
        date(session_start_date) as service_date,
        provider_ID 
    from {{ref('int__session')}} session
    join claims_with_id using (dependent_id)
),

provider as (
select *
from provider_prep
qualify row_number() over (partition by dependent_first_name, dependent_last_name, service_date order by provider_id) = 1
),

combined as (
select distinct 
    claims.session_ID,
    claims.Patient_Name,
    claims.dependent_first_name,
    claims.dependent_last_name,
    claims.Patient_ID,
    claims.Payer_Name,
    claims.Date_Of_Service,
    claims.Year_Month,
    claims.Place_of_Service_Code,
    claims.Service_Type,
    claims.Category,
    claims.Claim_Status,
    claims.Claim_Category,
    claims.Charges,
    claims.CPT_Code1,
    claims.Code_1_Charge,
    claims.CPT_Code2,
    claims.Code2_Charge,
    claims.CPT_code3,
    claims.Code_3_Charge,
    claims.CPT_code_4,
    claims.Code_4_Charge,
    claims.CPT_Code_X_Number_of_units,
    claims.Payer_Claim_Number,
    claims.Processed_Denied_Date,
    claims.Allowed_Amount,
    claims.Paid_Amount,
    claims.Copay,
    claims.Coins,
    claims.Deductible,
    claims.Total_PTR,
    claims.Check_EFT_Number,
    claims.Check_EFT,
    claims.EFT_Amount,
    claims.EFT_Date,
    claims.Comment,
    claims.Status,
    claims.Total_Insurance_Projection,
    claims.Total_Rev_Projection,
    claims.Session_Type,
    claims.Total_Allowable,
    claims.Total_Collected,
    claims.Total_Code_Charge,
    claims.Paid_Status,
    claims.Insurer_Code_Combo,
    claims.Insurer_Code_Lookup,
    claims.Charge_Code_1,
    claims.Charge_Code_1_Code,
    claims.Charge_Code_2,
    claims.Charge_Code_2_Code,
    claims.Charge_Code_3,
    claims.Charge_Code_3_Code,
    claims.Charge_Code_4,
    claims.Charge_Code_4_Code,
    claims.Hold_1,
    dep.dependent_ids,
    parent.parent_first_names,
    parent.parent_last_names,
    provider.provider_id
from claims_with_id claims
left join dependent_aggregated dep
    on claims.dependent_first_name = dep.dependent_first_name
    and claims.dependent_last_name = dep.dependent_last_name
left join parent_aggregated parent 
    on claims.dependent_first_name = parent.dependent_first_name
    and claims.dependent_last_name = parent.dependent_last_name
left join provider  
on claims.dependent_id = provider.dependent_id
and date(provider.service_date) = date(claims.Date_Of_Service)
),

final as (
select 
    session_ID,
    Patient_Name,
    dependent_first_name,
    dependent_last_name,
    Patient_ID,
    Payer_Name,
    Date_Of_Service,
    Year_Month,
    Service_Type,
    Category,
    Claim_Status,
    Claim_Category,
    Charges,
    CPT_Code1,
    Code_1_Charge,
    CPT_Code2,
    Code2_Charge,
    CPT_code3,
    Code_3_Charge,
    CPT_code_4,
    Code_4_Charge,
    CPT_Code_X_Number_of_units,
    Payer_Claim_Number,
    Processed_Denied_Date,
    Allowed_Amount,
    Paid_Amount,
    Copay,
    Coins,
    Deductible,
    Total_PTR,
    Check_EFT_Number,
    Check_EFT,
    EFT_Amount,
    EFT_Date,
    Comment,
    Status,
    Total_Insurance_Projection,
    Total_Rev_Projection,
    Session_Type,
    Total_Allowable,
    Total_Collected,
    Total_Code_Charge,
    Paid_Status,
    Insurer_Code_Combo,
    Insurer_Code_Lookup,
    Charge_Code_1,
    Charge_Code_1_Code,
    Charge_Code_2,
    Charge_Code_2_Code,
    Charge_Code_3,
    Charge_Code_3_Code,
    Charge_Code_4,
    Charge_Code_4_Code,
    Hold_1,
    dependent_ids,
    parent_first_names,
    parent_last_names,
    MAX(provider_id) AS provider_id
from combined 
GROUP BY ALL 
) 

select * from final 