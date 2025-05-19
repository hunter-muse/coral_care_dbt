with source as (
    select 
        deal.deal_id,
        deal.deal_name,
        CAST(deal.deal_created_date as date) as deal_created_date,
        CAST(school.school_created_date as date) as school_hubspot_created_date,
        CAST(deal.contact_id as string) as hubspot_school_id,
        school.school_contact_email AS hubspot_school_email,
        school.coral_school_id,
        deal.school_confirmed_date_time,
        -- School Pipeline Stages
        deal.date_entered_new_school_lead,
        deal.date_exited_new_school_lead,
        deal.date_entered_call_scheduled,
        deal.date_exited_call_scheduled,
        deal.date_entered_call_completed,
        deal.date_exited_call_completed,
        deal.date_entered_provider_matching,
        deal.date_exited_provider_matching,
        deal.date_entered_screening_confirmed,
        deal.date_exited_screening_confirmed,
        deal.date_entered_screening_complete_won,
        deal.date_entered_cold_school_pipeline AS date_entered_cold,
        deal.date_exited_cold_school_pipeline AS date_exited_cold,
        deal.date_entered_closed_lost_school_pipeline As date_entered_closed_lost
    from {{ ref('stg__hubspot__deal') }} deal
    INNER JOIN {{ ref('int__school')}} school
        ON deal.contact_id = school.hubspot_school_id
),

enriched as (
    select 
        *,
        -- New School Lead Stage
        case 
            when date_entered_new_school_lead is null and date_exited_new_school_lead is null then 'never_entered'
            when date_entered_new_school_lead is not null and date_exited_new_school_lead is null then 'current'
            when date_entered_new_school_lead = date_exited_new_school_lead then 'skipped'
            else 'completed'
        end as new_school_lead_status,

        -- Call Scheduled Stage
        case 
            when date_entered_call_scheduled is null and date_exited_call_scheduled is null then 'never_entered'
            when date_entered_call_scheduled is not null and date_exited_call_scheduled is null then 'current'
            when date_entered_call_scheduled = date_exited_call_scheduled then 'skipped'
            else 'completed'
        end as call_scheduled_status,

        -- Call Completed Stage
        case 
            when date_entered_call_completed is null and date_exited_call_completed is null then 'never_entered'
            when date_entered_call_completed is not null and date_exited_call_completed is null then 'current'
            when date_entered_call_completed = date_exited_call_completed then 'skipped'
            else 'completed'
        end as call_completed_status,

        -- Provider Matching Stage
        case 
            when date_entered_provider_matching is null and date_exited_provider_matching is null then 'never_entered'
            when date_entered_provider_matching is not null and date_exited_provider_matching is null then 'current'
            when date_entered_provider_matching = date_exited_provider_matching then 'skipped'
            else 'completed'
        end as provider_matching_status,

        -- Screening Confirmed Stage
        case 
            when date_entered_screening_confirmed is null and date_exited_screening_confirmed is null then 'never_entered'
            when date_entered_screening_confirmed is not null and date_exited_screening_confirmed is null then 'current'
            when date_entered_screening_confirmed = date_exited_screening_confirmed then 'skipped'
            else 'completed'
        end as screening_confirmed_status,

        -- Screening Complete (Won) Stage
        case 
            when date_entered_screening_complete_won is null then 'never_entered'
            when date_entered_screening_complete_won is not null then 'current'
            else 'completed'
        end as screening_complete_won_status,

        -- Cold Stage
        case 
            when date_entered_cold is null and date_exited_cold is null then 'never_entered'
            when date_entered_cold is not null and date_exited_cold is null then 'current'
            when date_entered_cold = date_exited_cold then 'skipped'
            else 'completed'
        end as cold_status,

        -- Closed Lost Stage
        case 
            when date_entered_closed_lost is null then 'never_entered'
            when date_entered_closed_lost is not null then 'current'
            else 'completed'
        end as closed_lost_status,

        -- Time calculations for each stage
        ROUND(
            CAST(DATEDIFF('second', date_entered_new_school_lead, 
                COALESCE(date_exited_new_school_lead, CURRENT_TIMESTAMP())) AS FLOAT) * (1/3600), 2
        ) as hours_in_new_school_lead,

        ROUND(
            CAST(DATEDIFF('second', date_entered_call_scheduled, 
                COALESCE(date_exited_call_scheduled, CURRENT_TIMESTAMP())) AS FLOAT) * (1/3600), 2
        ) as hours_in_call_scheduled,

        ROUND(
            CAST(DATEDIFF('second', date_entered_call_completed, 
                COALESCE(date_exited_call_completed, CURRENT_TIMESTAMP())) AS FLOAT) * (1/3600), 2
        ) as hours_in_call_completed,

        ROUND(
            CAST(DATEDIFF('second', date_entered_provider_matching, 
                COALESCE(date_exited_provider_matching, CURRENT_TIMESTAMP())) AS FLOAT) * (1/3600), 2
        ) as hours_in_provider_matching,

        ROUND(
            CAST(DATEDIFF('second', date_entered_screening_confirmed, 
                COALESCE(date_exited_screening_confirmed, CURRENT_TIMESTAMP())) AS FLOAT) * (1/3600), 2
        ) as hours_in_screening_confirmed,

        -- ROUND(
        --     CAST(DATEDIFF('second', date_entered_screening_complete_won, 
        --         COALESCE(date_exited_screening_complete_won, CURRENT_TIMESTAMP())) AS FLOAT) * (1/3600), 2
        -- ) as hours_in_screening_complete_won,

        ROUND(
            CAST(DATEDIFF('second', date_entered_cold, 
                COALESCE(date_exited_cold, CURRENT_TIMESTAMP())) AS FLOAT) * (1/3600), 2
        ) as hours_in_cold

        -- ROUND(
        --     CAST(DATEDIFF('second', date_entered_closed_lost, 
        --         COALESCE(date_exited_closed_lost, CURRENT_TIMESTAMP())) AS FLOAT) * (1/3600), 2
        -- ) as hours_in_closed_lost
    from source
),

current_stage_summary as (
    select 
        deal_id,
        deal_created_date,
        hubspot_school_id,
        case
            when new_school_lead_status = 'current' then 'New School Lead'
            when call_scheduled_status = 'current' then 'Call Scheduled'
            when call_completed_status = 'current' then 'Call Completed'
            when provider_matching_status = 'current' then 'Provider Matching'
            when screening_confirmed_status = 'current' then 'Screening Confirmed'
            when screening_complete_won_status = 'current' then 'Screening Complete (Won)'
            when cold_status = 'current' then 'Cold'
            when closed_lost_status = 'current' then 'Closed Lost'
        end as current_stage,
        case
            when new_school_lead_status = 'current' then hours_in_new_school_lead
            when call_scheduled_status = 'current' then hours_in_call_scheduled
            when call_completed_status = 'current' then hours_in_call_completed
            when provider_matching_status = 'current' then hours_in_provider_matching
            when screening_confirmed_status = 'current' then hours_in_screening_confirmed
            --when screening_complete_won_status = 'current' then hours_in_screening_complete_won
            when cold_status = 'current' then hours_in_cold
            --when closed_lost_status = 'current' then hours_in_closed_lost
        end as hours_in_current_stage
    from enriched
),

stage_timing_summary as (
    select
        deal_id,
        hubspot_school_id,
        deal_created_date,
        -- Total time in funnel (in hours)
        ROUND(
            COALESCE(hours_in_new_school_lead, 0) +
            COALESCE(hours_in_call_scheduled, 0) +
            COALESCE(hours_in_call_completed, 0) +
            COALESCE(hours_in_provider_matching, 0) +
            COALESCE(hours_in_screening_confirmed, 0) +
            --COALESCE(hours_in_screening_complete_won, 0) +
            COALESCE(hours_in_cold, 0) +
            --COALESCE(hours_in_closed_lost, 0),
            2
        ) as total_hours_in_funnel,
        -- Longest stage duration
        GREATEST(
            COALESCE(hours_in_new_school_lead, 0),
            COALESCE(hours_in_call_scheduled, 0),
            COALESCE(hours_in_call_completed, 0),
            COALESCE(hours_in_provider_matching, 0),
            COALESCE(hours_in_screening_confirmed, 0),
            --COALESCE(hours_in_screening_complete_won, 0),
            COALESCE(hours_in_cold, 0)
            --COALESCE(hours_in_closed_lost, 0)
        ) as longest_stage_duration
    from enriched
),

stage_statuses as (
    select 
        deal_id,
        hubspot_school_id,
        'new_school_lead' as stage_name,
        new_school_lead_status as status,
        1 as stage_order
    from enriched
    where new_school_lead_status = 'completed'
    
    union all
    
    select 
        deal_id,
        hubspot_school_id,
        'call_scheduled' as stage_name,
        call_scheduled_status as status,
        2 as stage_order
    from enriched
    where call_scheduled_status = 'completed'
    
    union all
    
    select 
        deal_id,
        hubspot_school_id,
        'call_completed' as stage_name,
        call_completed_status as status,
        3 as stage_order
    from enriched
    where call_completed_status = 'completed'
    
    union all
    
    select 
        deal_id,
        hubspot_school_id,
        'provider_matching' as stage_name,
        provider_matching_status as status,
        4 as stage_order
    from enriched
    where provider_matching_status = 'completed'
    
    union all
    
    select 
        deal_id,
        hubspot_school_id,
        'screening_confirmed' as stage_name,
        screening_confirmed_status as status,
        5 as stage_order
    from enriched
    where screening_confirmed_status = 'completed'
    
    union all
    
    select 
        deal_id,
        hubspot_school_id,
        'screening_complete_won' as stage_name,
        screening_complete_won_status as status,
        6 as stage_order
    from enriched
    where screening_complete_won_status = 'completed'
    
    union all
    
    select 
        deal_id,
        hubspot_school_id,
        'cold' as stage_name,
        cold_status as status,
        7 as stage_order
    from enriched
    where cold_status = 'completed'
    
    union all
    
    select 
        deal_id,
        hubspot_school_id,
        'closed_lost' as stage_name,
        closed_lost_status as status,
        8 as stage_order
    from enriched
    where closed_lost_status = 'completed'
),

funnel_progression as (
    select
        deal_id,
        hubspot_school_id,
        LISTAGG(stage_name, ' -> ') within group (order by stage_order) as progression_path
    from stage_statuses
    group by deal_id, hubspot_school_id
)

-- Final output
select distinct 
    e.*,
    cs.current_stage,
    cs.hours_in_current_stage,
    sts.total_hours_in_funnel,
    sts.longest_stage_duration,
    fp.progression_path
from enriched e
left join current_stage_summary cs on e.deal_id = cs.deal_id
left join stage_timing_summary sts on e.deal_id = sts.deal_id
left join funnel_progression fp on e.deal_id = fp.deal_id
order by 2,1