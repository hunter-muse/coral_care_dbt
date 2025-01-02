with source as (
    select deal.*, parent_enriched.* from {{ ref('stg__hubspot__deal') }} deal
    INNER JOIN {{ ref('stg__hubspot__contact_parent')}} parent
    ON deal.contact_id = parent.record_id 
    LEFT JOIN {{ ref('int__parent')}} parent_enriched
    ON parent.record_id = parent_enriched.hubspot_parent_id
),

enriched as (
    select 
        deal_id, 
        contact_id,
        parent_id, 
        -- Opportunities Unengaged Stage
        case 
            when date_entered_opportunities_unengaged is null and date_exited_opportunities_unengaged is null then 'never_entered'
            when date_entered_opportunities_unengaged is not null and date_exited_opportunities_unengaged is null then 'current'
            when date_entered_opportunities_unengaged = date_exited_opportunities_unengaged then 'skipped'
            else 'completed'
        end as opportunities_unengaged_status,

        -- Leads In Progress Stage
        case 
            when date_entered_leads_in_progress is null and date_exited_leads_in_progress is null then 'never_entered'
            when date_entered_leads_in_progress is not null and date_exited_leads_in_progress is null then 'current'
            when date_entered_leads_in_progress = date_exited_leads_in_progress then 'skipped'
            else 'completed'
        end as leads_in_progress_status,

        -- Match In Progress Stage
        case 
            when date_entered_match_in_progress is null and date_exited_match_in_progress is null then 'never_entered'
            when date_entered_match_in_progress is not null and date_exited_match_in_progress is null then 'current'
            when date_entered_match_in_progress = date_exited_match_in_progress then 'skipped'
            else 'completed'
        end as match_in_progress_status,

        -- Match Shared Stage
        case 
            when date_entered_match_shared is null and date_exited_match_shared is null then 'never_entered'
            when date_entered_match_shared is not null and date_exited_match_shared is null then 'current'
            when date_entered_match_shared = date_exited_match_shared then 'skipped'
            else 'completed'
        end as match_shared_status,

        -- Match Pending Stage
        case 
            when date_entered_match_pending is null and date_exited_match_pending is null then 'never_entered'
            when date_entered_match_pending is not null and date_exited_match_pending is null then 'current'
            when date_entered_match_pending = date_exited_match_pending then 'skipped'
            else 'completed'
        end as match_pending_status,

        -- Appointment Booked/Closed Won Stage
        case 
            when date_entered_appointment_booked_closed_won is null and date_exited_appointment_booked_closed_won is null then 'never_entered'
            when date_entered_appointment_booked_closed_won is not null and date_exited_appointment_booked_closed_won is null then 'current'
            when date_entered_appointment_booked_closed_won = date_exited_appointment_booked_closed_won then 'skipped'
            else 'completed'
        end as appointment_booked_closed_won_status,

        -- Closed Lost Stage
        case 
            when date_entered_closed_lost is null and date_exited_closed_lost is null then 'never_entered'
            when date_entered_closed_lost is not null and date_exited_closed_lost is null then 'current'
            when date_entered_closed_lost = date_exited_closed_lost then 'skipped'
            else 'completed'
        end as closed_lost_status,
-- Time calculations
-- Opportunities Unengaged
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_opportunities_unengaged,
            COALESCE(date_exited_opportunities_unengaged, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_opportunities_unengaged,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_opportunities_unengaged,
            COALESCE(date_exited_opportunities_unengaged, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_opportunities_unengaged,

-- Leads In Progress
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_leads_in_progress,
            COALESCE(date_exited_leads_in_progress, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_leads_in_progress,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_leads_in_progress,
            COALESCE(date_exited_leads_in_progress, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_leads_in_progress,

-- Match In Progress
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_match_in_progress,
            COALESCE(date_exited_match_in_progress, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_match_in_progress,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_match_in_progress,
            COALESCE(date_exited_match_in_progress, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_match_in_progress,

-- Match Shared
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_match_shared,
            COALESCE(date_exited_match_shared, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_match_shared,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_match_shared,
            COALESCE(date_exited_match_shared, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_match_shared,

-- Match Pending
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_match_pending,
            COALESCE(date_exited_match_pending, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_match_pending,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_match_pending,
            COALESCE(date_exited_match_pending, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_match_pending,

-- Appointment Booked/Closed Won
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_appointment_booked_closed_won,
            COALESCE(date_exited_appointment_booked_closed_won, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_appointment_booked_closed_won,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_appointment_booked_closed_won,
            COALESCE(date_exited_appointment_booked_closed_won, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_appointment_booked_closed_won,

-- Closed Lost
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_closed_lost,
            COALESCE(date_exited_closed_lost, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_closed_lost,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_closed_lost,
            COALESCE(date_exited_closed_lost, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_closed_lost
    from source
),

-- Add these useful views
current_stage_summary as (
    select 
        deal_id,
        contact_id,
        case
            when opportunities_unengaged_status = 'current' then 'Opportunities Unengaged'
            when leads_in_progress_status = 'current' then 'Leads In Progress'
            when match_in_progress_status = 'current' then 'Match In Progress'
            when match_shared_status = 'current' then 'Match Shared'
            when match_pending_status = 'current' then 'Match Pending'
            when appointment_booked_closed_won_status = 'current' then 'Appointment Booked/Won'
            when closed_lost_status = 'current' then 'Closed Lost'
        end as current_stage,
        case
            when opportunities_unengaged_status = 'current' then hours_in_opportunities_unengaged
            when leads_in_progress_status = 'current' then hours_in_leads_in_progress
            when match_in_progress_status = 'current' then hours_in_match_in_progress
            when match_shared_status = 'current' then hours_in_match_shared
            when match_pending_status = 'current' then hours_in_match_pending
            when appointment_booked_closed_won_status = 'current' then hours_in_appointment_booked_closed_won
            when closed_lost_status = 'current' then hours_in_closed_lost
        end as hours_in_current_stage
    from enriched
),

stage_timing_summary as (
    select
        deal_id,
        contact_id,
        -- Total time in funnel
        ROUND(
            COALESCE(days_in_opportunities_unengaged, 0) +
            COALESCE(days_in_leads_in_progress, 0) +
            COALESCE(days_in_match_in_progress, 0) +
            COALESCE(days_in_match_shared, 0) +
            COALESCE(days_in_match_pending, 0) +
            COALESCE(days_in_appointment_booked_closed_won, 0) +
            COALESCE(days_in_closed_lost, 0),
            2
        ) as total_days_in_funnel,
        -- Longest stage
        GREATEST(
            COALESCE(days_in_opportunities_unengaged, 0),
            COALESCE(days_in_leads_in_progress, 0),
            COALESCE(days_in_match_in_progress, 0),
            COALESCE(days_in_match_shared, 0),
            COALESCE(days_in_match_pending, 0),
            COALESCE(days_in_appointment_booked_closed_won, 0),
            COALESCE(days_in_closed_lost, 0)
        ) as longest_stage_duration
    from enriched
),

stage_statuses as (
    select 
        deal_id,
        contact_id,
        'opportunities_unengaged_status' as stage_name,
        opportunities_unengaged_status as status,
        1 as stage_order
    from enriched
    where opportunities_unengaged_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'leads_in_progress_status' as stage_name,
        leads_in_progress_status as status,
        2 as stage_order
    from enriched
    where leads_in_progress_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'match_in_progress_status' as stage_name,
        match_in_progress_status as status,
        3 as stage_order
    from enriched
    where match_in_progress_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'match_shared_status' as stage_name,
        match_shared_status as status,
        4 as stage_order
    from enriched
    where match_shared_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'match_pending_status' as stage_name,
        match_pending_status as status,
        5 as stage_order
    from enriched
    where match_pending_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'appointment_booked_closed_won_status' as stage_name,
        appointment_booked_closed_won_status as status,
        6 as stage_order
    from enriched
    where appointment_booked_closed_won_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'closed_lost_status' as stage_name,
        closed_lost_status as status,
        7 as stage_order
    from enriched
    where closed_lost_status = 'completed'
),

funnel_progression as (
    select
        deal_id,
        contact_id,
        LISTAGG(stage_name, ' -> ') within group (order by stage_order) as progression_path
    from stage_statuses
    group by deal_id, contact_id
)

-- Final output with all the enriched data plus summaries
select 
    e.*,
    cs.current_stage,
    cs.hours_in_current_stage,
    sts.total_days_in_funnel,
    sts.longest_stage_duration,
    fp.progression_path
from enriched e
left join current_stage_summary cs on e.deal_id = cs.deal_id
left join stage_timing_summary sts on e.deal_id = sts.deal_id
left join funnel_progression fp on e.deal_id = fp.deal_id
order by 2,1