with source as (
    select 
        deal.deal_id,
        CAST(deal.deaL_created_date as date) as deal_created_date,
        CAST(parent.parent_hubspot_created_date as date) as parent_hubspot_created_date,
        CAST(deal.contact_id as string) as contact_id,
        parent.parent_email AS hubspot_parent_email,
        parent_enriched.parent_email as coral_parent_email,
        parent_enriched.coral_parent_id,
        deal.date_entered_opportunities_unengaged,
        deal.date_exited_opportunities_unengaged,
        deal.date_entered_match_in_progress,
        deal.date_exited_match_in_progress,
        deal.date_entered_match_me,
        deal.date_exited_match_me,
        deal.date_entered_match_pending,
        deal.date_exited_match_pending,
        deal.date_entered_eval_booked,
        deal.date_exited_eval_booked,
        deal.date_entered_eval_complete,
        deal.date_exited_eval_complete,
        deal.date_entered_ongoing_sessions_scheduled,
        deal.date_exited_ongoing_sessions_scheduled,
        deal.date_entered_closed_lost,
        deal.date_exited_closed_lost,
        deal.date_entered_closed_lost_ongoing_treatment_not_recommended,
        deal.date_exited_closed_lost_ongoing_treatment_not_recommended,
        closed_lost_families_reason,
        closed_lost_families_reason_deprecated
    from {{ ref('stg__hubspot__deal') }} deal
    LEFT JOIN {{ ref('stg__hubspot__contact_parent')}} parent
        ON deal.contact_id = parent.record_id 
    LEFT JOIN {{ ref('int__parent')}} parent_enriched
        ON parent.record_id = parent_enriched.hubspot_parent_id
    where deal.deal_id = '34789435410'
),

enriched as (
    select  
        deal_id, 
        deaL_created_date, 
        parent_hubspot_created_date,
        contact_id,
        coral_parent_id,
        hubspot_parent_email,
        coral_parent_email,
        -- Add all date fields
        date_entered_opportunities_unengaged,
        date_exited_opportunities_unengaged,
        date_entered_match_in_progress,
        date_exited_match_in_progress,
        date_entered_match_me,
        date_exited_match_me,
        date_entered_match_pending,
        date_exited_match_pending,
        date_entered_eval_booked,
        date_exited_eval_booked,
        date_entered_eval_complete,
        date_exited_eval_complete,
        date_entered_ongoing_sessions_scheduled,
        date_exited_ongoing_sessions_scheduled,
        date_entered_closed_lost,
        date_exited_closed_lost,
        date_entered_closed_lost_ongoing_treatment_not_recommended,
        date_exited_closed_lost_ongoing_treatment_not_recommended,
        coalesce(closed_lost_families_reason, closed_lost_families_reason_deprecated) as closed_lost_families_reason,
        
        -- Opportunities Unengaged Stage
        case 
            when date_entered_opportunities_unengaged is null and date_exited_opportunities_unengaged is null then 'never_entered'
            when date_entered_opportunities_unengaged is not null and date_exited_opportunities_unengaged is null then 'current'
            when date_entered_opportunities_unengaged = date_exited_opportunities_unengaged then 'skipped'
            else 'completed'
        end as opportunities_unengaged_status,

        -- Match In Progress Stage
        case 
            when date_entered_match_in_progress is null and date_exited_match_in_progress is null then 'never_entered'
            when date_entered_match_in_progress is not null and date_exited_match_in_progress is null then 'current'
            when date_entered_match_in_progress = date_exited_match_in_progress then 'skipped'
            else 'completed'
        end as match_in_progress_status,

        -- Match Me Stage
        case 
            when date_entered_match_me is null and date_exited_match_me is null then 'never_entered'
            when date_entered_match_me is not null and date_exited_match_me is null then 'current'
            when date_entered_match_me = date_exited_match_me then 'skipped'
            else 'completed'
        end as match_me_status,

        -- Match Pending Stage
        case 
            when date_entered_match_pending is null and date_exited_match_pending is null then 'never_entered'
            when date_entered_match_pending is not null and date_exited_match_pending is null then 'current'
            when date_entered_match_pending = date_exited_match_pending then 'skipped'
            else 'completed'
        end as match_pending_status,

        -- Evaluation Booked Stage
        case 
            when date_entered_eval_booked is null and date_exited_eval_booked is null then 'never_entered'
            when date_entered_eval_booked is not null and date_exited_eval_booked is null then 'current'
            when date_entered_eval_booked = date_exited_eval_booked then 'skipped'
            else 'completed'
        end as eval_booked_status,

        -- Evaluation Complete Stage
        case 
            when date_entered_eval_complete is null and date_exited_eval_complete is null then 'never_entered'
            when date_entered_eval_complete is not null and date_exited_eval_complete is null then 'current'
            when date_entered_eval_complete = date_exited_eval_complete then 'skipped'
            else 'completed'
        end as eval_complete_status,

        -- Ongoing Sessions Scheduled Stage (Closed Won)
        case 
            when date_entered_ongoing_sessions_scheduled is null and date_exited_ongoing_sessions_scheduled is null then 'never_entered'
            when date_entered_ongoing_sessions_scheduled is not null and date_exited_ongoing_sessions_scheduled is null then 'current'
            when date_entered_ongoing_sessions_scheduled = date_exited_ongoing_sessions_scheduled then 'skipped'
            else 'completed'
        end as ongoing_sessions_scheduled_status,

        -- Closed Lost Stage
        case 
            when date_entered_closed_lost is null and date_exited_closed_lost is null then 'never_entered'
            when date_entered_closed_lost is not null and date_exited_closed_lost is null then 'current'
            when date_entered_closed_lost = date_exited_closed_lost then 'skipped'
            else 'completed'
        end as closed_lost_status,

        -- Closed Lost (Ongoing Treatment Not Recommended) Stage
        case 
            when date_entered_closed_lost_ongoing_treatment_not_recommended is null and date_exited_closed_lost_ongoing_treatment_not_recommended is null then 'never_entered'
            when date_entered_closed_lost_ongoing_treatment_not_recommended is not null and date_exited_closed_lost_ongoing_treatment_not_recommended is null then 'current'
            when date_entered_closed_lost_ongoing_treatment_not_recommended = date_exited_closed_lost_ongoing_treatment_not_recommended then 'skipped'
            else 'completed'
        end as closed_lost_ongoing_treatment_not_recommended_status,

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

-- Match Me
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_match_me,
            COALESCE(date_exited_match_me, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_match_me,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_match_me,
            COALESCE(date_exited_match_me, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_match_me,

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

-- Evaluation Booked
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_eval_booked,
            COALESCE(date_exited_eval_booked, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_eval_booked,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_eval_booked,
            COALESCE(date_exited_eval_booked, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_eval_booked,

-- Evaluation Complete
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_eval_complete,
            COALESCE(date_exited_eval_complete, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_eval_complete,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_eval_complete,
            COALESCE(date_exited_eval_complete, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_eval_complete,

-- Ongoing Sessions Scheduled (Closed Won)
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_ongoing_sessions_scheduled,
            COALESCE(date_exited_ongoing_sessions_scheduled, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_ongoing_sessions_scheduled,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_ongoing_sessions_scheduled,
            COALESCE(date_exited_ongoing_sessions_scheduled, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_ongoing_sessions_scheduled,

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
) as days_in_closed_lost,

-- Closed Lost (Ongoing Treatment Not Recommended)
ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_closed_lost_ongoing_treatment_not_recommended,
            COALESCE(date_exited_closed_lost_ongoing_treatment_not_recommended, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/3600),
    2
) as hours_in_closed_lost_ongoing_treatment_not_recommended,

ROUND(
    CAST(
        DATEDIFF(
            'second',
            date_entered_closed_lost_ongoing_treatment_not_recommended,
            COALESCE(date_exited_closed_lost_ongoing_treatment_not_recommended, CURRENT_TIMESTAMP())
        ) AS FLOAT
    ) * (1/86400),
    2
) as days_in_closed_lost_ongoing_treatment_not_recommended
    from source
),

-- Add these useful views
current_stage_summary as (
    select 
        deal_id,
        contact_id,
        case
            -- First check terminal stages
            when date_entered_closed_lost_ongoing_treatment_not_recommended is not null and 
                (date_exited_closed_lost_ongoing_treatment_not_recommended is null or 
                 date_exited_closed_lost_ongoing_treatment_not_recommended < date_entered_closed_lost_ongoing_treatment_not_recommended) 
                then 'Closed Lost (Ongoing Treatment Not Recommended)'
            when date_entered_closed_lost is not null and 
                (date_exited_closed_lost is null or 
                 date_exited_closed_lost < date_entered_closed_lost) 
                then 'Closed Lost'
            when date_entered_ongoing_sessions_scheduled is not null and 
                (date_exited_ongoing_sessions_scheduled is null or 
                 date_exited_ongoing_sessions_scheduled < date_entered_ongoing_sessions_scheduled) 
                then 'Ongoing Sessions Scheduled (Closed Won)'
                
            -- Then check if any stages are current (in reverse order - most advanced first)
            when eval_complete_status = 'current' or
                 (date_entered_eval_complete is not null and 
                  date_exited_eval_complete < date_entered_eval_complete)
                then 'Evaluation Complete'
            when eval_booked_status = 'current' or
                 (date_entered_eval_booked is not null and 
                  date_exited_eval_booked < date_entered_eval_booked)
                then 'Evaluation Booked'
            when match_pending_status = 'current' or
                 (date_entered_match_pending is not null and 
                  date_exited_match_pending < date_entered_match_pending)
                then 'Match Pending'
            when match_me_status = 'current' or
                 (date_entered_match_me is not null and 
                  date_exited_match_me < date_entered_match_me)
                then 'Match Me'
            when match_in_progress_status = 'current' or
                 (date_entered_match_in_progress is not null and 
                  date_exited_match_in_progress < date_entered_match_in_progress)
                then 'Match In-Progress'
            when opportunities_unengaged_status = 'current' or
                 (date_entered_opportunities_unengaged is not null and 
                  date_exited_opportunities_unengaged < date_entered_opportunities_unengaged)
                then 'Opportunities (Unengaged)'
                
            -- If no current stages, check if any stages have been completed (in reverse order)
            when date_entered_closed_lost_ongoing_treatment_not_recommended is not null then 'Closed Lost (Ongoing Treatment Not Recommended)'
            when date_entered_closed_lost is not null then 'Closed Lost'
            when date_entered_ongoing_sessions_scheduled is not null then 'Ongoing Sessions Scheduled (Closed Won)'
            when date_entered_eval_complete is not null then 'Evaluation Complete'
            when date_entered_eval_booked is not null then 'Evaluation Booked'
            when date_entered_match_pending is not null then 'Match Pending'
            when date_entered_match_me is not null then 'Match Me'
            when date_entered_match_in_progress is not null then 'Match In-Progress'
            when date_entered_opportunities_unengaged is not null then 'Opportunities (Unengaged)'
            else null
        end as current_stage,
        
        -- Similar updates for hours_in_current_stage calculation
        case
            -- Terminal stages
            when date_entered_closed_lost_ongoing_treatment_not_recommended is not null and 
                (date_exited_closed_lost_ongoing_treatment_not_recommended is null or 
                 date_exited_closed_lost_ongoing_treatment_not_recommended < date_entered_closed_lost_ongoing_treatment_not_recommended) then 
                ROUND(CAST(DATEDIFF('second', date_entered_closed_lost_ongoing_treatment_not_recommended, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when date_entered_closed_lost is not null and 
                (date_exited_closed_lost is null or 
                 date_exited_closed_lost < date_entered_closed_lost) then 
                ROUND(CAST(DATEDIFF('second', date_entered_closed_lost, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when date_entered_ongoing_sessions_scheduled is not null and 
                (date_exited_ongoing_sessions_scheduled is null or 
                 date_exited_ongoing_sessions_scheduled < date_entered_ongoing_sessions_scheduled) then 
                ROUND(CAST(DATEDIFF('second', date_entered_ongoing_sessions_scheduled, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
                
            -- Other stages (in reverse order)
            when eval_complete_status = 'current' or
                 (date_entered_eval_complete is not null and 
                  date_exited_eval_complete < date_entered_eval_complete) then 
                ROUND(CAST(DATEDIFF('second', date_entered_eval_complete, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when eval_booked_status = 'current' or
                 (date_entered_eval_booked is not null and 
                  date_exited_eval_booked < date_entered_eval_booked) then 
                ROUND(CAST(DATEDIFF('second', date_entered_eval_booked, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when match_pending_status = 'current' or
                 (date_entered_match_pending is not null and 
                  date_exited_match_pending < date_entered_match_pending) then 
                ROUND(CAST(DATEDIFF('second', date_entered_match_pending, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when match_me_status = 'current' or
                 (date_entered_match_me is not null and 
                  date_exited_match_me < date_entered_match_me) then 
                ROUND(CAST(DATEDIFF('second', date_entered_match_me, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when match_in_progress_status = 'current' or
                 (date_entered_match_in_progress is not null and 
                  date_exited_match_in_progress < date_entered_match_in_progress) then 
                ROUND(CAST(DATEDIFF('second', date_entered_match_in_progress, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            when opportunities_unengaged_status = 'current' or
                 (date_entered_opportunities_unengaged is not null and 
                  date_exited_opportunities_unengaged < date_entered_opportunities_unengaged) then 
                ROUND(CAST(DATEDIFF('second', date_entered_opportunities_unengaged, CURRENT_TIMESTAMP()) AS FLOAT) * (1/3600), 2)
            else null
        end as hours_in_current_stage
    from enriched
),

stage_timing_summary as (
    select
        deal_id,
        contact_id,
        deaL_created_date,
        -- Total time in funnel
        ROUND(
            COALESCE(days_in_opportunities_unengaged, 0) +
            COALESCE(days_in_match_in_progress, 0) +
            COALESCE(days_in_match_me, 0) +
            COALESCE(days_in_match_pending, 0) +
            COALESCE(days_in_eval_booked, 0) +
            COALESCE(days_in_eval_complete, 0) +
            COALESCE(days_in_ongoing_sessions_scheduled, 0) +
            COALESCE(days_in_closed_lost, 0) +
            COALESCE(days_in_closed_lost_ongoing_treatment_not_recommended, 0),
            2
        ) as total_days_in_funnel,
        -- Longest stage
        GREATEST(
            COALESCE(days_in_opportunities_unengaged, 0),
            COALESCE(days_in_match_in_progress, 0),
            COALESCE(days_in_match_me, 0),
            COALESCE(days_in_match_pending, 0),
            COALESCE(days_in_eval_booked, 0),
            COALESCE(days_in_eval_complete, 0),
            COALESCE(days_in_ongoing_sessions_scheduled, 0),
            COALESCE(days_in_closed_lost, 0),
            COALESCE(days_in_closed_lost_ongoing_treatment_not_recommended, 0)
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
        'match_in_progress_status' as stage_name,
        match_in_progress_status as status,
        2 as stage_order
    from enriched
    where match_in_progress_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'match_me_status' as stage_name,
        match_me_status as status,
        3 as stage_order
    from enriched
    where match_me_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'match_pending_status' as stage_name,
        match_pending_status as status,
        4 as stage_order
    from enriched
    where match_pending_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'eval_booked_status' as stage_name,
        eval_booked_status as status,
        5 as stage_order
    from enriched
    where eval_booked_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'eval_complete_status' as stage_name,
        eval_complete_status as status,
        6 as stage_order
    from enriched
    where eval_complete_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'ongoing_sessions_scheduled_status' as stage_name,
        ongoing_sessions_scheduled_status as status,
        7 as stage_order
    from enriched
    where ongoing_sessions_scheduled_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'closed_lost_status' as stage_name,
        closed_lost_status as status,
        8 as stage_order
    from enriched
    where closed_lost_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        'closed_lost_ongoing_treatment_not_recommended_status' as stage_name,
        closed_lost_ongoing_treatment_not_recommended_status as status,
        9 as stage_order
    from enriched
    where closed_lost_ongoing_treatment_not_recommended_status = 'completed'
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