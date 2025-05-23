with provider_funnel as (
    select * from {{ ref('int__hubspot_provider_funnel') }}
),

combined as (
    select
        hubspot_provider_id,
        coral_provider_id,
        provider_state,
        provider_hubspot_created_date,
        
        -- Provider funnel data
        current_stage,
        provider_lead_status,
        hours_in_current_stage,
        total_days_in_funnel,
        longest_stage_duration,
        progression_path,
        
        -- Determine the overall status
        CASE
            -- Terminal states
            WHEN current_stage IN (
                'Closed Lost (Provider Recruiting)',
                'Disqualified (Provider Recruiting)'
            ) THEN 'Lost'
            
            -- Hold state
            WHEN current_stage = 'Hold (Provider Recruiting)' THEN 'On Hold'
            WHEN current_stage = 'Recruitment Complete (Provider Recruiting)' THEN 'Recruitment Complete'
            -- Active recruiting stages
            WHEN current_stage IN (
                'Pre-Launch (Provider Recruiting)',
                'Ready for Kickoff Call (Provider Recruiting)',
                'Pending Tasks (Provider Recruiting)',
                'Offer Letter (Provider Recruiting)',
                'Pending Referral/Specialty',
                'Clinical Interview (Provider Recruiting)',
                'Interview CX',
                'Interview Booked (Provider Recruiting)',
                'New Provider Lead (Provider Recruiting)',
                'Re-Activate (Provider Recruiting)',
                'Referrals (Provider Recruiting)'
            ) THEN 'Active Lead'
            
            ELSE 'Unknown'
        END as overall_status
from provider_funnel
)

select * from combined
order by coral_provider_id, hubspot_provider_id