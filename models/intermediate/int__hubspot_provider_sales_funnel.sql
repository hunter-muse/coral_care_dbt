with source as (
    select 
        deal.deal_id,
        deal.deaL_created_date,
        deal.deal_sequence,
        provider.provider_hubspot_created_date,
        provider_enriched.coral_provider_id,
        provider_enriched.state as provider_state, 
        provider_enriched.hubspot_provider_id,
        -- Provider Sales Stages
        deal.date_entered_discovery_call_provider_sales,
        deal.date_exited_discovery_call_provider_sales,
        deal.date_entered_clinical_interview_provider_sales,
        deal.date_exited_clinical_interview_provider_sales,
        deal.date_entered_pending_tasks_provider_sales,
        -- Commented out fields that will be added later
        -- deal.date_exited_pending_tasks_provider_sales,
        -- deal.date_entered_offer_letter_provider_sales,
        -- deal.date_exited_offer_letter_provider_sales,
        -- deal.date_entered_closed_won_provider_sales,
        -- deal.date_exited_closed_won_provider_sales,
        deal.date_entered_closed_lost_provider_sales
        -- deal.date_exited_closed_lost_provider_sales
    from {{ ref('stg__hubspot__deal') }} deal
    INNER JOIN {{ ref('stg__hubspot__contact_provider')}} provider
        ON deal.contact_id = provider.hubspot_provider_id  
    LEFT JOIN {{ ref('int__provider')}} provider_enriched
        ON provider.hubspot_provider_id = provider_enriched.hubspot_provider_id
),

-- Group deals by provider to combine sales data
provider_deals as (
    select
        hubspot_provider_id,
        coral_provider_id,
        provider_state,
        provider_hubspot_created_date,
        -- Sales pipeline fields
        MAX(date_entered_discovery_call_provider_sales) as date_entered_discovery_call_provider_sales,
        MAX(date_exited_discovery_call_provider_sales) as date_exited_discovery_call_provider_sales,
        MAX(date_entered_clinical_interview_provider_sales) as date_entered_clinical_interview_provider_sales,
        MAX(date_exited_clinical_interview_provider_sales) as date_exited_clinical_interview_provider_sales,
        MAX(date_entered_pending_tasks_provider_sales) as date_entered_pending_tasks_provider_sales,
        -- Commented out fields that will be added later
        -- MAX(date_exited_pending_tasks_provider_sales) as date_exited_pending_tasks_provider_sales,
        -- MAX(date_entered_offer_letter_provider_sales) as date_entered_offer_letter_provider_sales,
        -- MAX(date_exited_offer_letter_provider_sales) as date_exited_offer_letter_provider_sales,
        -- MAX(date_entered_closed_won_provider_sales) as date_entered_closed_won_provider_sales,
        -- MAX(date_exited_closed_won_provider_sales) as date_exited_closed_won_provider_sales,
        MAX(date_entered_closed_lost_provider_sales) as date_entered_closed_lost_provider_sales,
        -- MAX(date_exited_closed_lost_provider_sales) as date_exited_closed_lost_provider_sales,
        
        -- Use MIN of deal_created_date to get the earliest deal
        MIN(deaL_created_date) as first_deal_created_date
    from source
    group by hubspot_provider_id, coral_provider_id, provider_hubspot_created_date, provider_state
),

enriched as (
    select 
        hubspot_provider_id, 
        coral_provider_id,
        provider_state,
        provider_hubspot_created_date,
        
        -- All date fields
        date_entered_discovery_call_provider_sales,
        date_exited_discovery_call_provider_sales,
        date_entered_clinical_interview_provider_sales,
        date_exited_clinical_interview_provider_sales,
        date_entered_pending_tasks_provider_sales,
        -- Commented out fields that will be added later
        -- date_exited_pending_tasks_provider_sales,
        -- date_entered_offer_letter_provider_sales,
        -- date_exited_offer_letter_provider_sales,
        -- date_entered_closed_won_provider_sales,
        -- date_exited_closed_won_provider_sales,
        date_entered_closed_lost_provider_sales,
        -- date_exited_closed_lost_provider_sales,
        
        -- Provider Sales Stages Status
        -- Discovery Call Provider Sales Stage
        case 
            when date_entered_discovery_call_provider_sales is null and date_exited_discovery_call_provider_sales is null then 'never_entered'
            when date_entered_discovery_call_provider_sales is not null and date_exited_discovery_call_provider_sales is null then 'current'
            when date_entered_discovery_call_provider_sales = date_exited_discovery_call_provider_sales then 'skipped'
            else 'completed'
        end as discovery_call_provider_sales_status,
        
        -- Clinical Interview Provider Sales Stage
        case 
            when date_entered_clinical_interview_provider_sales is null and date_exited_clinical_interview_provider_sales is null then 'never_entered'
            when date_entered_clinical_interview_provider_sales is not null and date_exited_clinical_interview_provider_sales is null then 'current'
            when date_entered_clinical_interview_provider_sales = date_exited_clinical_interview_provider_sales then 'skipped'
            else 'completed'
        end as clinical_interview_provider_sales_status,
        
        -- Pending Tasks Provider Sales Stage
        case 
            when date_entered_pending_tasks_provider_sales is null then 'never_entered'
            when date_entered_pending_tasks_provider_sales is not null then 'current'
            else 'completed'
        end as pending_tasks_provider_sales_status,
        
        -- Commented out stages that will be added later
        /*
        -- Offer Letter Provider Sales Stage
        case 
            when date_entered_offer_letter_provider_sales is null and date_exited_offer_letter_provider_sales is null then 'never_entered'
            when date_entered_offer_letter_provider_sales is not null and date_exited_offer_letter_provider_sales is null then 'current'
            when date_entered_offer_letter_provider_sales = date_exited_offer_letter_provider_sales then 'skipped'
            else 'completed'
        end as offer_letter_provider_sales_status,
        
        -- Closed Won Provider Sales Stage
        case 
            when date_entered_closed_won_provider_sales is null and date_exited_closed_won_provider_sales is null then 'never_entered'
            when date_entered_closed_won_provider_sales is not null and date_exited_closed_won_provider_sales is null then 'current'
            when date_entered_closed_won_provider_sales = date_exited_closed_won_provider_sales then 'skipped'
            else 'completed'
        end as closed_won_provider_sales_status,
        */
        
        -- Closed Lost Provider Sales Stage
        case 
            when date_entered_closed_lost_provider_sales is null then 'never_entered'
            when date_entered_closed_lost_provider_sales is not null then 'current'
            else 'completed'
        end as closed_lost_provider_sales_status,
        
        -- Calculate hours in each stage
        -- Discovery Call Provider Sales Stage
        ROUND(
            CASE
                WHEN date_entered_discovery_call_provider_sales IS NULL THEN 0
                WHEN date_exited_discovery_call_provider_sales IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_discovery_call_provider_sales, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_discovery_call_provider_sales, date_exited_discovery_call_provider_sales) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_discovery_call_provider_sales,
        
        -- Clinical Interview Provider Sales Stage
        ROUND(
            CASE
                WHEN date_entered_clinical_interview_provider_sales IS NULL THEN 0
                WHEN date_exited_clinical_interview_provider_sales IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_clinical_interview_provider_sales, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_clinical_interview_provider_sales, date_exited_clinical_interview_provider_sales) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_clinical_interview_provider_sales,
        
        -- Pending Tasks Provider Sales Stage
        ROUND(
            CASE
                WHEN date_entered_pending_tasks_provider_sales IS NULL THEN 0
                ELSE 
                    CAST(DATEDIFF('second', date_entered_pending_tasks_provider_sales, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_pending_tasks_provider_sales,
        
        -- Commented out calculations that will be added later
        /*
        -- Offer Letter Provider Sales Stage
        ROUND(
            CASE
                WHEN date_entered_offer_letter_provider_sales IS NULL THEN 0
                WHEN date_exited_offer_letter_provider_sales IS NULL THEN 
                    CAST(DATEDIFF('second', date_entered_offer_letter_provider_sales, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600)
                ELSE 
                    CAST(DATEDIFF('second', date_entered_offer_letter_provider_sales, date_exited_offer_letter_provider_sales) AS FLOAT) * (1/3600)
            END, 2
        ) as hours_in_offer_letter_provider_sales,
        */
        
        -- Convert hours to days for each stage
        ROUND(hours_in_discovery_call_provider_sales / 24, 2) as days_in_discovery_call_provider_sales,
        ROUND(hours_in_clinical_interview_provider_sales / 24, 2) as days_in_clinical_interview_provider_sales,
        ROUND(hours_in_pending_tasks_provider_sales / 24, 2) as days_in_pending_tasks_provider_sales
        -- Commented out conversions that will be added later
        -- ROUND(hours_in_offer_letter_provider_sales / 24, 2) as days_in_offer_letter_provider_sales
    from provider_deals
),

current_stage_summary as (
    select 
        hubspot_provider_id,
        coral_provider_id,
        case
            -- First check terminal sales stages
            when date_entered_closed_lost_provider_sales is not null then 'Closed Lost (Provider Sales)'
            -- Commented out stages that will be added later
            -- when date_entered_closed_won_provider_sales is not null and 
            --     (date_exited_closed_won_provider_sales is null or 
            --      date_exited_closed_won_provider_sales < date_entered_closed_won_provider_sales) 
            --     then 'Closed Won (Provider Sales)'
            
            -- Then check if any sales stages are current (in reverse order - most advanced first)
            -- Commented out stages that will be added later
            -- when offer_letter_provider_sales_status = 'current' or
            --      (date_entered_offer_letter_provider_sales is not null and 
            --       date_exited_offer_letter_provider_sales < date_entered_offer_letter_provider_sales)
            --     then 'Offer Letter (Provider Sales)'
            when pending_tasks_provider_sales_status = 'current' or
                 (date_entered_pending_tasks_provider_sales is not null)
                then 'Pending Tasks (Provider Sales)'
            when clinical_interview_provider_sales_status = 'current' or
                 (date_entered_clinical_interview_provider_sales is not null and 
                  date_exited_clinical_interview_provider_sales < date_entered_clinical_interview_provider_sales)
                then 'Clinical Interview (Provider Sales)'
            when discovery_call_provider_sales_status = 'current' or
                 (date_entered_discovery_call_provider_sales is not null and 
                  date_exited_discovery_call_provider_sales < date_entered_discovery_call_provider_sales)
                then 'Discovery Call (Provider Sales)'
            
            -- If no current stages, check if any sales stages have been completed (in reverse order)
            -- Commented out stages that will be added later
            -- when date_entered_closed_won_provider_sales is not null then 'Closed Won (Provider Sales)'
            -- when date_entered_offer_letter_provider_sales is not null then 'Offer Letter (Provider Sales)'
            when date_entered_pending_tasks_provider_sales is not null then 'Pending Tasks (Provider Sales)'
            when date_entered_clinical_interview_provider_sales is not null then 'Clinical Interview (Provider Sales)'
            when date_entered_discovery_call_provider_sales is not null then 'Discovery Call (Provider Sales)'
            else null
        end as current_stage,
        
        -- Hours in current stage calculation
        case
            -- Terminal sales stages
            when date_entered_closed_lost_provider_sales is not null then 
                ROUND(CAST(DATEDIFF('second', date_entered_closed_lost_provider_sales, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            
            -- Commented out stages that will be added later
            -- when date_entered_closed_won_provider_sales is not null and 
            --     (date_exited_closed_won_provider_sales is null or 
            --      date_exited_closed_won_provider_sales < date_entered_closed_won_provider_sales) then 
            --     ROUND(CAST(DATEDIFF('second', date_entered_closed_won_provider_sales, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            
            -- Provider Sales Stages (in reverse order)
            -- Commented out stages that will be added later
            -- when offer_letter_provider_sales_status = 'current' or
            --      (date_entered_offer_letter_provider_sales is not null and 
            --       date_exited_offer_letter_provider_sales < date_entered_offer_letter_provider_sales) then 
            --     hours_in_offer_letter_provider_sales
            when pending_tasks_provider_sales_status = 'current' or
                 (date_entered_pending_tasks_provider_sales is not null) then 
                hours_in_pending_tasks_provider_sales
            when clinical_interview_provider_sales_status = 'current' or
                 (date_entered_clinical_interview_provider_sales is not null and 
                  date_exited_clinical_interview_provider_sales < date_entered_clinical_interview_provider_sales) then 
                hours_in_clinical_interview_provider_sales
            when discovery_call_provider_sales_status = 'current' or
                 (date_entered_discovery_call_provider_sales is not null and 
                  date_exited_discovery_call_provider_sales < date_entered_discovery_call_provider_sales) then 
                hours_in_discovery_call_provider_sales
        end as hours_in_current_stage
    from enriched
),

stage_timing_summary as (
    select
        hubspot_provider_id,
        -- Total time in funnel
        ROUND(
            -- Provider Sales Stages
            COALESCE(days_in_discovery_call_provider_sales, 0) +
            COALESCE(days_in_clinical_interview_provider_sales, 0) +
            COALESCE(days_in_pending_tasks_provider_sales, 0),
            -- Commented out stages that will be added later
            -- COALESCE(days_in_offer_letter_provider_sales, 0),
            2
        ) as total_days_in_funnel,
        -- Longest stage
        GREATEST(
            -- Provider Sales Stages
            COALESCE(days_in_discovery_call_provider_sales, 0),
            COALESCE(days_in_clinical_interview_provider_sales, 0),
            COALESCE(days_in_pending_tasks_provider_sales, 0)
            -- Commented out stages that will be added later
            -- COALESCE(days_in_offer_letter_provider_sales, 0)
        ) as longest_stage_duration
    from enriched
),

stage_statuses as (
    select 
        hubspot_provider_id, 
        coral_provider_id,
        'discovery_call_provider_sales_status' as stage_name,
        discovery_call_provider_sales_status as status,
        1 as stage_order
    from enriched
    where discovery_call_provider_sales_status = 'completed'
    
    union all
    
    select 
        hubspot_provider_id,
        coral_provider_id,
        'clinical_interview_provider_sales_status' as stage_name,
        clinical_interview_provider_sales_status as status,
        2 as stage_order
    from enriched
    where clinical_interview_provider_sales_status = 'completed'
    
    union all
    
    select 
        hubspot_provider_id,
        coral_provider_id,
        'pending_tasks_provider_sales_status' as stage_name,
        pending_tasks_provider_sales_status as status,
        3 as stage_order
    from enriched
    where pending_tasks_provider_sales_status = 'completed'
    
    -- Commented out stages that will be added later
    /*
    union all
    
    select 
        hubspot_provider_id,
        coral_provider_id,
        'offer_letter_provider_sales_status' as stage_name,
        offer_letter_provider_sales_status as status,
        4 as stage_order
    from enriched
    where offer_letter_provider_sales_status = 'completed'
    
    union all
    
    select 
        hubspot_provider_id,
        coral_provider_id,
        'closed_won_provider_sales_status' as stage_name,
        closed_won_provider_sales_status as status,
        5 as stage_order
    from enriched
    where closed_won_provider_sales_status = 'completed'
    */
),

funnel_progression as (
    select
        hubspot_provider_id, 
        coral_provider_id,
        LISTAGG(stage_name, ' -> ') within group (order by stage_order) as progression_path
    from stage_statuses
    group by hubspot_provider_id, coral_provider_id
)

-- Final output with all the enriched data plus summaries
select distinct 
    e.*,
    cs.current_stage,
    CASE 
        WHEN cs.current_stage IS NULL THEN 'Inactive_Lead'
        WHEN cs.current_stage = 'Closed Lost (Provider Sales)' THEN 'Inactive_Lead'
        -- Commented out stages that will be added later
        -- WHEN cs.current_stage = 'Closed Won (Provider Sales)' THEN 'Completed_Sales'
        ELSE 'Active_Lead' 
    END AS provider_sales_status,
    cs.hours_in_current_stage,
    sts.total_days_in_funnel,
    sts.longest_stage_duration,
    fp.progression_path
from enriched e
left join current_stage_summary cs on e.hubspot_provider_id = cs.hubspot_provider_id
left join stage_timing_summary sts on e.hubspot_provider_id = sts.hubspot_provider_id
left join funnel_progression fp on e.hubspot_provider_id = fp.hubspot_provider_id
order by 2,1