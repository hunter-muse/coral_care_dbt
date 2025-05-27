with provider_funnel as (
    select * from {{ ref('int__hubspot_provider_funnel') }}
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
        -- Terminal states
        WHEN c.current_stage IN (
            'Closed Lost (Provider Recruiting)',
            'Disqualified (Provider Recruiting)'
        ) THEN 'Closed'
        
        -- Active stages
        WHEN c.current_stage IS NOT NULL THEN 'Recruiting'
        
        ELSE 'Unknown'
    END as funnel_type,
    
    -- Combined funnel information
    c.overall_status,
    
    -- Current stage
    c.current_stage as recruiting_current_stage,
    
    -- Provider funnel summary fields
    c.hours_in_current_stage,
    c.total_days_in_funnel,
    c.longest_stage_duration,
    c.progression_path,
    
    -- All detailed provider funnel fields
    r.date_entered_referrals_provider_recruiting,
    r.date_exited_referrals_provider_recruiting,
    r.date_entered_re_activate_provider_recruiting,
    r.date_exited_re_activate_provider_recruiting,
    -- r.date_entered_lead_magnet_provider_recruiting,
    -- r.date_exited_lead_magnet_provider_recruiting,
    r.date_entered_new_provider_lead_provider_recruiting,
    r.date_exited_new_provider_lead_provider_recruiting,
    r.date_entered_interview_booked_provider_recruiting,
    r.date_exited_interview_booked_provider_recruiting,
    r.date_entered_interview_cx_provider_recruiting,
    r.date_exited_interview_cx_provider_recruiting, 
    -- r.date_entered_post_interview_provider_recruiting, --LOOK INTO THIS 
    -- r.date_exited_post_interview_provider_recruiting, --LOOK INTO THIS 
    r.date_entered_clinical_interview_provider_recruiting,
    r.date_exited_clinical_interview_provider_recruiting,
    r.date_entered_pending_referral_specialties_provider_recruiting,
    r.date_exited_pending_referral_specialties_provider_recruiting,
    r.date_entered_offer_letter_provider_recruiting,
    r.date_exited_offer_letter_provider_recruiting,
    r.date_entered_pending_tasks_provider_recruiting, 
    r.date_exited_pending_tasks_provider_recruiting,  
    r.date_entered_ready_for_kickoff_provider_recruiting,
    r.date_exited_ready_for_kickoff_provider_recruiting,
    -- r.date_entered_onboarding_call_provider_recruiting, 
    -- r.date_exited_onboarding_call_provider_recruiting, 
    r.date_entered_pre_launch_provider_recruiting, 
    r.date_exited_pre_launch_provider_recruiting,
    r.date_entered_recruitment_complete_provider_recruiting,
    r.date_exited_recruitment_complete_provider_recruiting,
    r.date_entered_hold_provider_recruiting,
    r.date_exited_hold_provider_recruiting,
    r.date_entered_cold_provider_recruiting,
    r.date_exited_cold_provider_recruiting,
    r.date_entered_closed_lost_provider_recruiting,
    r.date_entered_disqualified_provider_recruiting,
    
    -- Rename onboarding fields with LEGACY_ prefix
    -- r.date_entered_ready_to_onboard_provider_onboarding as date_entered_LEGACY_ready_to_onboard_provider_onboarding,
    -- r.date_exited_ready_to_onboard_provider_onboarding as date_exited_LEGACY_ready_to_onboard_provider_onboarding,
    -- r.date_entered_pending_onboarding_tasks_provider_onboarding as date_entered_LEGACY_pending_onboarding_tasks_provider_onboarding,
    -- r.date_exited_pending_onboarding_tasks_provider_onboarding as date_exited_LEGACY_pending_onboarding_tasks_provider_onboarding,
    -- r.date_entered_schedule_onboarding_call_provider_onboarding as date_entered_LEGACY_schedule_onboarding_call_provider_onboarding,
    -- r.date_exited_schedule_onboarding_call_provider_onboarding as date_exited_LEGACY_schedule_onboarding_call_provider_onboarding,
    -- r.date_entered_onboarding_complete_provider_onboarding as date_entered_LEGACY_onboarding_complete_provider_onboarding,
    -- r.date_exited_onboarding_complete_provider_onboarding as date_exited_LEGACY_onboarding_complete_provider_onboarding,
    -- r.date_entered_checkr_fail_provider_onboarding as date_entered_LEGACY_checkr_fail_provider_onboarding,
    -- r.date_exited_checkr_fail_provider_onboarding as date_exited_LEGACY_checkr_fail_provider_onboarding,
    -- r.date_entered_hold_provider_onboarding as date_entered_LEGACY_hold_provider_onboarding,
    -- r.date_exited_hold_provider_onboarding as date_exited_LEGACY_hold_provider_onboarding,
    -- r.date_entered_cold_provider_onboarding as date_entered_LEGACY_cold_provider_onboarding,
    -- r.date_exited_cold_provider_onboarding as date_exited_LEGACY_cold_provider_onboarding,
    -- r.date_entered_closed_lost_provider_onboarding as date_entered_LEGACY_closed_lost_provider_onboarding,
    -- r.date_entered_disqualified_provider_onboarding as date_entered_LEGACY_disqualified_provider_onboarding,
    -- r.date_entered_pre_launch_provider_onboarding as date_entered_LEGACY_pre_launch_provider_onboarding,
    -- r.date_exited_pre_launch_provider_onboarding as date_exited_LEGACY_pre_launch_provider_onboarding,
    
    -- Provider funnel status fields (update onboarding status fields)
    r.referrals_provider_recruiting_status,
    r.re_activate_provider_recruiting_status,
    -- r.lead_magnet_provider_recruiting_status,
    r.new_provider_lead_provider_recruiting_status,
    r.interview_booked_provider_recruiting_status,
    r.interview_cx_provider_recruiting_status,
    --r.post_interview_provider_recruiting_status,
    r.clinical_interview_provider_recruiting_status,
    r.pending_referral_specialties_provider_recruiting_status,
    r.offer_letter_provider_recruiting_status,
    r.pending_tasks_provider_recruiting_status,
    r.ready_for_kickoff_provider_recruiting_status,
    -- r.onboarding_call_provider_recruiting_status,
    r.pre_launch_provider_recruiting_status,
    r.recruitment_complete_provider_recruiting_status,
    r.cold_provider_recruiting_status,
    r.hold_provider_recruiting_status,
    r.closed_lost_provider_recruiting_status,
    -- r.ready_to_onboard_provider_onboarding_status as LEGACY_ready_to_onboard_provider_onboarding_status,
    -- r.pending_onboarding_tasks_provider_onboarding_status as LEGACY_pending_onboarding_tasks_provider_onboarding_status,
    -- r.schedule_onboarding_call_provider_onboarding_status as LEGACY_schedule_onboarding_call_provider_onboarding_status,
    -- r.pre_launch_provider_onboarding_status as LEGACY_pre_launch_provider_onboarding_status,
    
    -- Time metrics (update onboarding metrics)
    r.hours_in_referrals_provider_recruiting,
    r.hours_in_re_activate_provider_recruiting,
    -- r.hours_in_lead_magnet_provider_recruiting,
    r.hours_in_new_provider_lead_provider_recruiting,
    r.hours_in_interview_booked_provider_recruiting,
    r.hours_in_interview_cx_provider_recruiting, 
    --r.hours_in_post_interview_provider_recruiting,
    r.hours_in_clinical_interview_provider_recruiting,
    r.hours_in_pending_referral_specialties_provider_recruiting,
    r.hours_in_offer_letter_provider_recruiting,
    r.hours_in_pending_tasks_provider_recruiting,
    r.hours_in_ready_for_kickoff_provider_recruiting,
    -- r.hours_in_onboarding_call_provider_recruiting,
    r.hours_in_pre_launch_provider_recruiting,
    r.hours_in_recruitment_complete_provider_recruiting,
    -- r.hours_in_pre_launch_provider_onboarding as hours_in_LEGACY_pre_launch_provider_onboarding,
    -- r.hours_in_ready_to_onboard_provider_onboarding as hours_in_LEGACY_ready_to_onboard_provider_onboarding,
    -- r.hours_in_pending_onboarding_tasks_provider_onboarding as hours_in_LEGACY_pending_onboarding_tasks_provider_onboarding,
    -- r.hours_in_schedule_onboarding_call_provider_onboarding as hours_in_LEGACY_schedule_onboarding_call_provider_onboarding,
    -- r.hours_in_checkr_fail_provider_onboarding as hours_in_LEGACY_checkr_fail_provider_onboarding,
    -- r.hours_in_hold_provider_onboarding as hours_in_LEGACY_hold_provider_onboarding,
    
    r.days_in_referrals_provider_recruiting,
    r.days_in_re_activate_provider_recruiting,
    -- r.days_in_lead_magnet_provider_recruiting,
    r.days_in_new_provider_lead_provider_recruiting,
    r.days_in_interview_booked_provider_recruiting,
    r.days_in_interview_cx_provider_recruiting,
    r.days_in_post_interview_provider_recruiting,
    r.days_in_clinical_interview_provider_recruiting,
    r.days_in_pending_referral_specialties_provider_recruiting,
    r.days_in_offer_letter_provider_recruiting,
    r.days_in_pending_tasks_provider_recruiting,
    r.days_in_ready_for_kickoff_provider_recruiting,
    --r.days_in_onboarding_call_provider_recruiting,
    r.days_in_pre_launch_provider_recruiting,
    r.days_in_recruitment_complete_provider_recruiting,
    -- r.days_in_pre_launch_provider_onboarding as days_in_LEGACY_pre_launch_provider_onboarding,
    -- r.days_in_ready_to_onboard_provider_onboarding as days_in_LEGACY_ready_to_onboard_provider_onboarding,
    -- r.days_in_pending_onboarding_tasks_provider_onboarding as days_in_LEGACY_pending_onboarding_tasks_provider_onboarding,
    -- r.days_in_schedule_onboarding_call_provider_onboarding as days_in_LEGACY_schedule_onboarding_call_provider_onboarding,
    -- r.days_in_checkr_fail_provider_onboarding as days_in_LEGACY_checkr_fail_provider_onboarding,
    -- r.days_in_hold_provider_onboarding as days_in_LEGACY_hold_provider_onboarding,
    --r.total_days_to_onboarding_complete,
    --r.total_days_to_closed_lost_onboarding,
    r.total_days_to_closed_lost_recruiting

from combined_funnel c
left join provider_funnel r
    on c.hubspot_provider_id = r.hubspot_provider_id
order by c.coral_provider_id, c.hubspot_provider_id