with source as (
    select deal.*, provider_enriched.provider_id from {{ ref('stg__hubspot__deal') }} deal
    INNER JOIN {{ ref('stg__hubspot__contact_provider')}} provider
    ON deal.contact_id = provider.record_id 
    LEFT JOIN {{ ref('int__provider')}} provider_enriched
    ON provider.record_id = provider_enriched.hubspot_provider_id
),

enriched as (
    select 
        deal_id, 
        contact_id,
        provider_id,
        case 
            when date_entered_referrals_provider_recruiting is null and date_exited_referrals_provider_recruiting is null then 'never_entered'
            when date_entered_referrals_provider_recruiting is not null and date_exited_referrals_provider_recruiting is null then 'current'
            when date_entered_referrals_provider_recruiting = date_exited_referrals_provider_recruiting then 'skipped'
            else 'completed'
        end as referrals_provider_recruiting_status,

        -- New Provider Lead Stage
        case 
            when date_entered_new_provider_lead_provider_recruiting is null and date_exited_new_provider_lead_provider_recruiting is null then 'never_entered'
            when date_entered_new_provider_lead_provider_recruiting is not null and date_exited_new_provider_lead_provider_recruiting is null then 'current'
            when date_entered_new_provider_lead_provider_recruiting = date_exited_new_provider_lead_provider_recruiting then 'skipped'
            else 'completed'
        end as new_provider_lead_provider_recruiting_status,

        -- Interview Booked Stage
        case 
            when date_entered_interview_booked_provider_recruiting is null and date_exited_interview_booked_provider_recruiting is null then 'never_entered'
            when date_entered_interview_booked_provider_recruiting is not null and date_exited_interview_booked_provider_recruiting is null then 'current'
            when date_entered_interview_booked_provider_recruiting = date_exited_interview_booked_provider_recruiting then 'skipped'
            else 'completed'
        end as interview_booked_provider_recruiting_status,

        -- Post Interview Stage
        case 
            when date_entered_post_interview_provider_recruiting is null and date_exited_post_interview_provider_recruiting is null then 'never_entered'
            when date_entered_post_interview_provider_recruiting is not null and date_exited_post_interview_provider_recruiting is null then 'current'
            when date_entered_post_interview_provider_recruiting = date_exited_post_interview_provider_recruiting then 'skipped'
            else 'completed'
        end as post_interview_provider_recruiting_status,

        -- Clinical Interview Stage
        case 
            when date_entered_clinical_interview_provider_recruiting is null and date_exited_clinical_interview_provider_recruiting is null then 'never_entered'
            when date_entered_clinical_interview_provider_recruiting is not null and date_exited_clinical_interview_provider_recruiting is null then 'current'
            when date_entered_clinical_interview_provider_recruiting = date_exited_clinical_interview_provider_recruiting then 'skipped'
            else 'completed'
        end as clinical_interview_provider_recruiting_status,

        -- Offer Letter Stage
        case 
            when date_entered_offer_letter_provider_recruiting is null and date_exited_offer_letter_provider_recruiting is null then 'never_entered'
            when date_entered_offer_letter_provider_recruiting is not null and date_exited_offer_letter_provider_recruiting is null then 'current'
            when date_entered_offer_letter_provider_recruiting = date_exited_offer_letter_provider_recruiting then 'skipped'
            else 'completed'
        end as offer_letter_provider_recruiting_status,

        -- Recruitment Complete Stage
        case 
            when date_entered_recruitment_complete_provider_recruiting is null and date_exited_recruitment_complete_provider_recruiting is null then 'never_entered'
            when date_entered_recruitment_complete_provider_recruiting is not null and date_exited_recruitment_complete_provider_recruiting is null then 'current'
            when date_entered_recruitment_complete_provider_recruiting = date_exited_recruitment_complete_provider_recruiting then 'skipped'
            else 'completed'
        end as recruitment_complete_provider_recruiting_status,

        -- Cold Provider Recruiting Stage
        case 
            when date_entered_cold_provider_recruiting is null and date_exited_cold_provider_recruiting is null then 'never_entered'
            when date_entered_cold_provider_recruiting is not null and date_exited_cold_provider_recruiting is null then 'current'
            when date_entered_cold_provider_recruiting = date_exited_cold_provider_recruiting then 'skipped'
            else 'completed'
        end as cold_provider_recruiting_status,

        -- Closed Lost Provider Recruiting Stage
        case 
            when date_entered_closed_lost_provider_recruiting is null and date_exited_closed_lost_provider_recruiting is null then 'never_entered'
            when date_entered_closed_lost_provider_recruiting is not null and date_exited_closed_lost_provider_recruiting is null then 'current'
            when date_entered_closed_lost_provider_recruiting = date_exited_closed_lost_provider_recruiting then 'skipped'
            else 'completed'
        end as closed_lost_provider_recruiting_status,

        -- Provider Onboarding Stages
        -- Ready to Onboard Stage
        case 
            when date_entered_ready_to_onboard_provider_onboarding is null and date_exited_ready_to_onboard_provider_onboarding is null then 'never_entered'
            when date_entered_ready_to_onboard_provider_onboarding is not null and date_exited_ready_to_onboard_provider_onboarding is null then 'current'
            when date_entered_ready_to_onboard_provider_onboarding = date_exited_ready_to_onboard_provider_onboarding then 'skipped'
            else 'completed'
        end as ready_to_onboard_provider_onboarding_status,

        -- Pending Onboarding Tasks Stage
        case 
            when date_entered_pending_onboarding_tasks_provider_onboarding is null and date_exited_pending_onboarding_tasks_provider_onboarding is null then 'never_entered'
            when date_entered_pending_onboarding_tasks_provider_onboarding is not null and date_exited_pending_onboarding_tasks_provider_onboarding is null then 'current'
            when date_entered_pending_onboarding_tasks_provider_onboarding = date_exited_pending_onboarding_tasks_provider_onboarding then 'skipped'
            else 'completed'
        end as pending_onboarding_tasks_provider_onboarding_status,

        -- Schedule Onboarding Call Stage
        case 
            when date_entered_schedule_onboarding_call_provider_onboarding is null and date_exited_schedule_onboarding_call_provider_onboarding is null then 'never_entered'
            when date_entered_schedule_onboarding_call_provider_onboarding is not null and date_exited_schedule_onboarding_call_provider_onboarding is null then 'current'
            when date_entered_schedule_onboarding_call_provider_onboarding = date_exited_schedule_onboarding_call_provider_onboarding then 'skipped'
            else 'completed'
        end as schedule_onboarding_call_provider_onboarding_status,

        case   
            when date_entered_checkr_fail_provider_onboarding is null and date_exited_checkr_fail_provider_onboarding is null then 'never_entered'
            when date_entered_checkr_fail_provider_onboarding is not null and date_exited_checkr_fail_provider_onboarding is null then 'current'
            when date_entered_checkr_fail_provider_onboarding = date_exited_checkr_fail_provider_onboarding then 'skipped'
            else 'completed'
        end as checkr_fail_provider_onboarding_status, 

        -- Time calculations for each stage
        -- Provider Recruiting Stages
        -- Referrals
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_referrals_provider_recruiting,
                    COALESCE(date_exited_referrals_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_referrals_provider_recruiting,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_referrals_provider_recruiting,
                    COALESCE(date_exited_referrals_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_referrals_provider_recruiting,

        -- New Provider Lead
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_new_provider_lead_provider_recruiting,
                    COALESCE(date_exited_new_provider_lead_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_new_provider_lead_provider_recruiting,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_new_provider_lead_provider_recruiting,
                    COALESCE(date_exited_new_provider_lead_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_new_provider_lead_provider_recruiting,

        -- Interview Booked
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_interview_booked_provider_recruiting,
                    COALESCE(date_exited_interview_booked_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_interview_booked_provider_recruiting,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_interview_booked_provider_recruiting,
                    COALESCE(date_exited_interview_booked_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_interview_booked_provider_recruiting,

        -- Post Interview
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_post_interview_provider_recruiting,
                    COALESCE(date_exited_post_interview_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_post_interview_provider_recruiting,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_post_interview_provider_recruiting,
                    COALESCE(date_exited_post_interview_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_post_interview_provider_recruiting,

        -- Clinical Interview
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_clinical_interview_provider_recruiting,
                    COALESCE(date_exited_clinical_interview_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_clinical_interview_provider_recruiting,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_clinical_interview_provider_recruiting,
                    COALESCE(date_exited_clinical_interview_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_clinical_interview_provider_recruiting,

        -- Offer Letter
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_offer_letter_provider_recruiting,
                    COALESCE(date_exited_offer_letter_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_offer_letter_provider_recruiting,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_offer_letter_provider_recruiting,
                    COALESCE(date_exited_offer_letter_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_offer_letter_provider_recruiting,

        -- Recruitment Complete
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_recruitment_complete_provider_recruiting,
                    COALESCE(date_exited_recruitment_complete_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_recruitment_complete_provider_recruiting,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_recruitment_complete_provider_recruiting,
                    COALESCE(date_exited_recruitment_complete_provider_recruiting, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_recruitment_complete_provider_recruiting,

        -- Ready to Onboard
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_ready_to_onboard_provider_onboarding,
                    COALESCE(date_exited_ready_to_onboard_provider_onboarding, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_ready_to_onboard_provider_onboarding,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_ready_to_onboard_provider_onboarding,
                    COALESCE(date_exited_ready_to_onboard_provider_onboarding, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_ready_to_onboard_provider_onboarding,

        -- Pending Onboarding Tasks
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_pending_onboarding_tasks_provider_onboarding,
                    COALESCE(date_exited_pending_onboarding_tasks_provider_onboarding, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_pending_onboarding_tasks_provider_onboarding,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_pending_onboarding_tasks_provider_onboarding,
                    COALESCE(date_exited_pending_onboarding_tasks_provider_onboarding, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_pending_onboarding_tasks_provider_onboarding,

        -- Schedule Onboarding Call
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_schedule_onboarding_call_provider_onboarding,
                    COALESCE(date_exited_schedule_onboarding_call_provider_onboarding, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_schedule_onboarding_call_provider_onboarding,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_schedule_onboarding_call_provider_onboarding,
                    COALESCE(date_exited_schedule_onboarding_call_provider_onboarding, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_schedule_onboarding_call_provider_onboarding,

         -- Checkr Fail
        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_checkr_fail_provider_onboarding,
                    COALESCE(date_exited_checkr_fail_provider_onboarding, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/3600),
            2
        ) as hours_in_checkr_fail_provider_onboarding,

        ROUND(
            CAST(
                DATEDIFF(
                    'second',
                    date_entered_checkr_fail_provider_onboarding,
                    COALESCE(date_exited_checkr_fail_provider_onboarding, CURRENT_TIMESTAMP())
                ) AS FLOAT
            ) * (1/86400),
            2
        ) as days_in_checkr_fail_provider_onboarding,

    from source
),

current_stage_summary as (
    select 
        deal_id,
        contact_id,
        provider_id,
        case
            -- Provider Recruiting Stages
            when referrals_provider_recruiting_status = 'current' then 'Referrals (Provider Recruiting)'
            when new_provider_lead_provider_recruiting_status = 'current' then 'New Provider Lead (Provider Recruiting)'
            when interview_booked_provider_recruiting_status = 'current' then 'Interview Booked (Provider Recruiting)'
            when post_interview_provider_recruiting_status = 'current' then 'Post Interview (Provider Recruiting)'
            when clinical_interview_provider_recruiting_status = 'current' then 'Clinical Interview (Provider Recruiting)'
            when offer_letter_provider_recruiting_status = 'current' then 'Offer Letter (Provider Recruiting)'
            when recruitment_complete_provider_recruiting_status = 'current' then 'Recruitment Complete (Provider Recruiting)'
            when cold_provider_recruiting_status = 'current' then 'Cold (Provider Recruiting)'
            when closed_lost_provider_recruiting_status = 'current' then 'Closed Lost (Provider Recruiting)'
            -- Provider Onboarding Stages
            when ready_to_onboard_provider_onboarding_status = 'current' then 'Ready to Onboard (Provider Onboarding)'
            when pending_onboarding_tasks_provider_onboarding_status = 'current' then 'Pending Onboarding Tasks (Provider Onboarding)'
            when schedule_onboarding_call_provider_onboarding_status = 'current' then 'Schedule Onboarding Call (Provider Onboarding)'
            when checkr_fail_provider_onboarding_status = 'current' then 'Checkr Fail (Provider Onboarding)'
        end as current_stage,
        case
            -- Provider Recruiting Stages
            when referrals_provider_recruiting_status = 'current' then hours_in_referrals_provider_recruiting
            when new_provider_lead_provider_recruiting_status = 'current' then hours_in_new_provider_lead_provider_recruiting
            when interview_booked_provider_recruiting_status = 'current' then hours_in_interview_booked_provider_recruiting
            when post_interview_provider_recruiting_status = 'current' then hours_in_post_interview_provider_recruiting
            when clinical_interview_provider_recruiting_status = 'current' then hours_in_clinical_interview_provider_recruiting
            when offer_letter_provider_recruiting_status = 'current' then hours_in_offer_letter_provider_recruiting
            when recruitment_complete_provider_recruiting_status = 'current' then hours_in_recruitment_complete_provider_recruiting
            -- Provider Onboarding Stages
            when ready_to_onboard_provider_onboarding_status = 'current' then hours_in_ready_to_onboard_provider_onboarding
            when pending_onboarding_tasks_provider_onboarding_status = 'current' then hours_in_pending_onboarding_tasks_provider_onboarding
            when schedule_onboarding_call_provider_onboarding_status = 'current' then hours_in_schedule_onboarding_call_provider_onboarding
            when checkr_fail_provider_onboarding_status = 'current' then hours_in_checkr_fail_provider_onboarding
        end as hours_in_current_stage
    from enriched
),

stage_timing_summary as (
    select
        deal_id,
        contact_id,
        -- Total time in funnel
        ROUND(
            -- Provider Recruiting Stages
            COALESCE(days_in_referrals_provider_recruiting, 0) +
            COALESCE(days_in_new_provider_lead_provider_recruiting, 0) +
            COALESCE(days_in_interview_booked_provider_recruiting, 0) +
            COALESCE(days_in_post_interview_provider_recruiting, 0) +
            COALESCE(days_in_clinical_interview_provider_recruiting, 0) +
            COALESCE(days_in_offer_letter_provider_recruiting, 0) +
            COALESCE(days_in_recruitment_complete_provider_recruiting, 0) +
            -- Provider Onboarding Stages
            COALESCE(days_in_ready_to_onboard_provider_onboarding, 0) +
            COALESCE(days_in_pending_onboarding_tasks_provider_onboarding, 0) +
            COALESCE(days_in_schedule_onboarding_call_provider_onboarding, 0) + 
            COALESCE(days_in_checkr_fail_provider_onboarding,0),
            2
        ) as total_days_in_funnel,
        -- Longest stage
        GREATEST(
            -- Provider Recruiting Stages
            COALESCE(days_in_referrals_provider_recruiting, 0),
            COALESCE(days_in_new_provider_lead_provider_recruiting, 0),
            COALESCE(days_in_interview_booked_provider_recruiting, 0),
            COALESCE(days_in_post_interview_provider_recruiting, 0),
            COALESCE(days_in_clinical_interview_provider_recruiting, 0),
            COALESCE(days_in_offer_letter_provider_recruiting, 0),
            COALESCE(days_in_recruitment_complete_provider_recruiting, 0),
            -- Provider Onboarding Stages
            COALESCE(days_in_ready_to_onboard_provider_onboarding, 0),
            COALESCE(days_in_pending_onboarding_tasks_provider_onboarding, 0),
            COALESCE(days_in_schedule_onboarding_call_provider_onboarding, 0),
            COALESCE(days_in_checkr_fail_provider_onboarding,0)
        ) as longest_stage_duration
    from enriched
),

stage_statuses as (
    select 
        deal_id,
        contact_id,
        provider_id,
        'referrals_provider_recruiting_status' as stage_name,
        referrals_provider_recruiting_status as status,
        1 as stage_order
    from enriched
    where referrals_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        provider_id,
        'new_provider_lead_provider_recruiting_status' as stage_name,
        new_provider_lead_provider_recruiting_status as status,
        2 as stage_order
    from enriched
    where new_provider_lead_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id, 
        provider_id,
        'interview_booked_provider_recruiting_status' as stage_name,
        interview_booked_provider_recruiting_status as status,
        3 as stage_order
    from enriched
    where interview_booked_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        provider_id,
        'post_interview_provider_recruiting_status' as stage_name,
        post_interview_provider_recruiting_status as status,
        4 as stage_order
    from enriched
    where post_interview_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        provider_id,
        'clinical_interview_provider_recruiting_status' as stage_name,
        clinical_interview_provider_recruiting_status as status,
        5 as stage_order
    from enriched
    where clinical_interview_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        provider_id,
        'offer_letter_provider_recruiting_status' as stage_name,
        offer_letter_provider_recruiting_status as status,
        6 as stage_order
    from enriched
    where offer_letter_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        provider_id,
        'recruitment_complete_provider_recruiting_status' as stage_name,
        recruitment_complete_provider_recruiting_status as status,
        7 as stage_order
    from enriched
    where recruitment_complete_provider_recruiting_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        provider_id,
        'ready_to_onboard_provider_onboarding_status' as stage_name,
        ready_to_onboard_provider_onboarding_status as status,
        8 as stage_order
    from enriched
    where ready_to_onboard_provider_onboarding_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        provider_id,
        'pending_onboarding_tasks_provider_onboarding_status' as stage_name,
        pending_onboarding_tasks_provider_onboarding_status as status,
        9 as stage_order
    from enriched
    where pending_onboarding_tasks_provider_onboarding_status = 'completed'
    
    union all
    
    select 
        deal_id,
        contact_id,
        provider_id,
        'schedule_onboarding_call_provider_onboarding_status' as stage_name,
        schedule_onboarding_call_provider_onboarding_status as status,
        10 as stage_order
    from enriched
    where schedule_onboarding_call_provider_onboarding_status = 'completed'

        union all
    
    select 
        deal_id,
        contact_id, 
        provider_id,
        'checkr_fail_provider_onboarding_status' as stage_name,
        checkr_fail_provider_onboarding_status as status,
        11 as stage_order
    from enriched
    where checkr_fail_provider_onboarding_status = 'completed'
),

funnel_progression as (
    select
        deal_id,
        contact_id, 
        provider_id,
        LISTAGG(stage_name, ' -> ') within group (order by stage_order) as progression_path
    from stage_statuses
    group by deal_id, contact_id, provider_id
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