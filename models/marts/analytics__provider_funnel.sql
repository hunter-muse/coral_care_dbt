with recruiting_funnel as (
    select * from {{ ref('int__hubspot_provider_funnel') }}
),

sales_funnel as (
    select * from {{ ref('int__hubspot_provider_sales_funnel') }}
),

combined_funnel as (
    select * from {{ ref('int__hubspot_provider_combined_funnel') }}
)

select
    -- Common identifier fields
    c.hubspot_provider_id,
    c.coral_provider_id,
    c.provider_state,
    c.provider_hubspot_created_date,
    
    -- Funnel type indicator
    CASE
        -- Check for onboarding stages first
        WHEN c.recruiting_current_stage IN (
            'Pre-Launch (Provider Onboarding)',
            'Ready to Onboard (Provider Onboarding)',
            'Pending Onboarding Tasks (Provider Onboarding)',
            'Schedule Onboarding Call (Provider Onboarding)',
            'Onboarding Complete (Provider Onboarding)',
            'Hold (Provider Onboarding)',
            'Checkr Fail (Provider Onboarding)',
            'Closed Lost (Provider Onboarding)',
            'Disqualified (Provider Onboarding)'
        ) THEN 'Onboarding'
        
        -- Then check for recruiting stages
        WHEN c.recruiting_current_stage IS NOT NULL AND 
             c.recruiting_current_stage NOT IN (
                'Closed Lost (Provider Recruiting)',
                'Disqualified (Provider Recruiting)'
             ) THEN 'Recruiting'
        
        -- Then check for sales stages
        WHEN c.sales_current_stage IS NOT NULL AND
             c.sales_current_stage != 'Closed Lost (Provider Sales)' THEN 'Sales'
        
        -- Handle terminal states
        WHEN c.recruiting_current_stage IN (
            'Closed Lost (Provider Recruiting)',
            'Disqualified (Provider Recruiting)'
        ) OR c.sales_current_stage = 'Closed Lost (Provider Sales)' THEN 'Closed'
        
        ELSE 'Unknown'
    END as funnel_type,
    
    -- Combined funnel information
    c.active_funnel,
    c.overall_status,
    
    -- Consolidated current stage (either recruiting or sales)
    COALESCE(c.recruiting_current_stage, c.sales_current_stage) as current_stage,
    
    -- Recruiting funnel summary fields
    c.recruiting_current_stage,
    c.recruiting_lead_status,
    c.recruiting_hours_in_current_stage,
    c.recruiting_total_days_in_funnel,
    c.recruiting_longest_stage_duration,
    c.recruiting_progression_path,
    
    -- Sales funnel summary fields
    c.sales_current_stage,
    c.sales_lead_status,
    c.sales_hours_in_current_stage,
    c.sales_total_days_in_funnel,
    c.sales_longest_stage_duration,
    c.sales_progression_path,
    
    -- All detailed recruiting funnel fields
    r.date_entered_referrals_provider_recruiting,
    r.date_exited_referrals_provider_recruiting,
    r.date_entered_lead_magnet_provider_recruiting,
    r.date_exited_lead_magnet_provider_recruiting,
    r.date_entered_new_provider_lead_provider_recruiting,
    r.date_exited_new_provider_lead_provider_recruiting,
    r.date_entered_interview_booked_provider_recruiting,
    r.date_exited_interview_booked_provider_recruiting,
    r.date_entered_post_interview_provider_recruiting,
    r.date_exited_post_interview_provider_recruiting,
    r.date_entered_clinical_interview_provider_recruiting,
    r.date_exited_clinical_interview_provider_recruiting,
    r.date_entered_offer_letter_provider_recruiting,
    r.date_exited_offer_letter_provider_recruiting,
    r.date_entered_recruitment_complete_provider_recruiting,
    r.date_exited_recruitment_complete_provider_recruiting,
    r.date_entered_cold_provider_recruiting,
    r.date_exited_cold_provider_recruiting,
    r.date_entered_closed_lost_provider_recruiting,
    r.date_entered_disqualified_provider_recruiting,
    
    -- Rename onboarding fields with LEGACY_ prefix
    r.date_entered_ready_to_onboard_provider_onboarding as date_entered_LEGACY_ready_to_onboard_provider_onboarding,
    r.date_exited_ready_to_onboard_provider_onboarding as date_exited_LEGACY_ready_to_onboard_provider_onboarding,
    r.date_entered_pending_onboarding_tasks_provider_onboarding as date_entered_LEGACY_pending_onboarding_tasks_provider_onboarding,
    r.date_exited_pending_onboarding_tasks_provider_onboarding as date_exited_LEGACY_pending_onboarding_tasks_provider_onboarding,
    r.date_entered_schedule_onboarding_call_provider_onboarding as date_entered_LEGACY_schedule_onboarding_call_provider_onboarding,
    r.date_exited_schedule_onboarding_call_provider_onboarding as date_exited_LEGACY_schedule_onboarding_call_provider_onboarding,
    r.date_entered_onboarding_complete_provider_onboarding as date_entered_LEGACY_onboarding_complete_provider_onboarding,
    r.date_exited_onboarding_complete_provider_onboarding as date_exited_LEGACY_onboarding_complete_provider_onboarding,
    r.date_entered_checkr_fail_provider_onboarding as date_entered_LEGACY_checkr_fail_provider_onboarding,
    r.date_exited_checkr_fail_provider_onboarding as date_exited_LEGACY_checkr_fail_provider_onboarding,
    r.date_entered_hold_provider_onboarding as date_entered_LEGACY_hold_provider_onboarding,
    r.date_exited_hold_provider_onboarding as date_exited_LEGACY_hold_provider_onboarding,
    r.date_entered_cold_provider_onboarding as date_entered_LEGACY_cold_provider_onboarding,
    r.date_exited_cold_provider_onboarding as date_exited_LEGACY_cold_provider_onboarding,
    r.date_entered_closed_lost_provider_onboarding as date_entered_LEGACY_closed_lost_provider_onboarding,
    r.date_entered_disqualified_provider_onboarding as date_entered_LEGACY_disqualified_provider_onboarding,
    r.date_entered_pre_launch_provider_onboarding as date_entered_LEGACY_pre_launch_provider_onboarding,
    r.date_exited_pre_launch_provider_onboarding as date_exited_LEGACY_pre_launch_provider_onboarding,
    
    -- Recruiting funnel status fields (update onboarding status fields)
    r.referrals_provider_recruiting_status,
    r.lead_magnet_provider_recruiting_status,
    r.new_provider_lead_provider_recruiting_status,
    r.interview_booked_provider_recruiting_status,
    r.post_interview_provider_recruiting_status,
    r.clinical_interview_provider_recruiting_status,
    r.offer_letter_provider_recruiting_status,
    r.recruitment_complete_provider_recruiting_status,
    r.cold_provider_recruiting_status,
    r.closed_lost_provider_recruiting_status,
    r.ready_to_onboard_provider_onboarding_status as LEGACY_ready_to_onboard_provider_onboarding_status,
    r.pending_onboarding_tasks_provider_onboarding_status as LEGACY_pending_onboarding_tasks_provider_onboarding_status,
    r.schedule_onboarding_call_provider_onboarding_status as LEGACY_schedule_onboarding_call_provider_onboarding_status,
    r.pre_launch_provider_onboarding_status as LEGACY_pre_launch_provider_onboarding_status,
    
    -- Time metrics (update onboarding metrics)
    r.hours_in_referrals_provider_recruiting,
    r.hours_in_lead_magnet_provider_recruiting,
    r.hours_in_new_provider_lead_provider_recruiting,
    r.hours_in_interview_booked_provider_recruiting,
    r.hours_in_post_interview_provider_recruiting,
    r.hours_in_clinical_interview_provider_recruiting,
    r.hours_in_offer_letter_provider_recruiting,
    r.hours_in_recruitment_complete_provider_recruiting,
    r.hours_in_pre_launch_provider_onboarding as hours_in_LEGACY_pre_launch_provider_onboarding,
    r.hours_in_ready_to_onboard_provider_onboarding as hours_in_LEGACY_ready_to_onboard_provider_onboarding,
    r.hours_in_pending_onboarding_tasks_provider_onboarding as hours_in_LEGACY_pending_onboarding_tasks_provider_onboarding,
    r.hours_in_schedule_onboarding_call_provider_onboarding as hours_in_LEGACY_schedule_onboarding_call_provider_onboarding,
    r.hours_in_checkr_fail_provider_onboarding as hours_in_LEGACY_checkr_fail_provider_onboarding,
    r.hours_in_hold_provider_onboarding as hours_in_LEGACY_hold_provider_onboarding,
    
    r.days_in_referrals_provider_recruiting,
    r.days_in_lead_magnet_provider_recruiting,
    r.days_in_new_provider_lead_provider_recruiting,
    r.days_in_interview_booked_provider_recruiting,
    r.days_in_post_interview_provider_recruiting,
    r.days_in_clinical_interview_provider_recruiting,
    r.days_in_offer_letter_provider_recruiting,
    r.days_in_recruitment_complete_provider_recruiting,
    r.days_in_pre_launch_provider_onboarding as days_in_LEGACY_pre_launch_provider_onboarding,
    r.days_in_ready_to_onboard_provider_onboarding as days_in_LEGACY_ready_to_onboard_provider_onboarding,
    r.days_in_pending_onboarding_tasks_provider_onboarding as days_in_LEGACY_pending_onboarding_tasks_provider_onboarding,
    r.days_in_schedule_onboarding_call_provider_onboarding as days_in_LEGACY_schedule_onboarding_call_provider_onboarding,
    r.days_in_checkr_fail_provider_onboarding as days_in_LEGACY_checkr_fail_provider_onboarding,
    r.days_in_hold_provider_onboarding as days_in_LEGACY_hold_provider_onboarding,
    
    -- All detailed sales funnel fields
    s.date_entered_discovery_call_provider_sales,
    s.date_exited_discovery_call_provider_sales,
    s.date_entered_clinical_interview_provider_sales,
    s.date_exited_clinical_interview_provider_sales,
    s.date_entered_pending_tasks_provider_sales,
    s.date_entered_closed_lost_provider_sales,
    
    -- Sales funnel status fields
    s.discovery_call_provider_sales_status,
    s.clinical_interview_provider_sales_status,
    s.pending_tasks_provider_sales_status,
    s.closed_lost_provider_sales_status,
    
    -- Sales funnel time metrics
    s.hours_in_discovery_call_provider_sales,
    s.hours_in_clinical_interview_provider_sales,
    s.hours_in_pending_tasks_provider_sales,
    
    s.days_in_discovery_call_provider_sales,
    s.days_in_clinical_interview_provider_sales,
    s.days_in_pending_tasks_provider_sales

from combined_funnel c
left join recruiting_funnel r
    on c.hubspot_provider_id = r.hubspot_provider_id
left join sales_funnel s
    on c.hubspot_provider_id = s.hubspot_provider_id
order by c.coral_provider_id, c.hubspot_provider_id