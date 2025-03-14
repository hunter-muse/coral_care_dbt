with most_recent_evaluation as (
    select 
        outcome.session_id,
        dependent.dependent_id,
        outcome.date_of_service,
        outcome.evaluation_summary,
        NULL AS GOALS_LONG_TERM_TIMELINE,
        outcome.treatment_plan_recommendations,
        outcome.recommendation,
        -- Add row number to identify the most recent record per dependent
        row_number() over (partition by dependent.dependent_id order by outcome.date_of_service desc) as rn
    from 
    {{ref('stg__google_sheets__evaluation_outcome')}} outcome 
    left join 
    {{ref('int__session')}} session
    on outcome.session_id = session.session_id
    left join 
    {{ref('stg__bubble__dependent')}} dependent
    on session.dependent_id = dependent.dependent_id
),

most_recent_session as (
    select 
        outcome.session_id,
        dependent.dependent_id,
        outcome.date_of_service,
        NULL AS evaluation_summary,
        outcome.GOALS_LONG_TERM_TIMELINE,
        outcome.treatment_plan_recommendations,
        outcome.recommendation,
        -- Add row number to identify the most recent record per dependent
        row_number() over (partition by dependent.dependent_id order by outcome.date_of_service desc) as rn
    from 
    {{ref('stg__google_sheets__session_outcome')}} outcome 
    left join 
    {{ref('int__session')}} session
    on outcome.session_id = session.session_id
    left join 
    {{ref('stg__bubble__dependent')}} dependent
    on session.dependent_id = dependent.dependent_id
),

combined_data as (
    select
        dependent_id,
        session_id,
        date_of_service,
        evaluation_summary as evaluation_summary_from_session,
        treatment_plan_recommendations as treatment_plan_recommendation_from_session,
        recommendation as evaluation_recommendation_from_session,
        GOALS_LONG_TERM_TIMELINE as goals_long_term_timeline_from_session,
        'most recent session' as source,
        NULL as evaluation_summary_from_evaluation,
        NULL as treatment_plan_recommendation_from_evaluation,
        NULL as evaluation_recommendation,
        NULL as goals_long_term_timeline,
        date_of_service as session_date,
        NULL as evaluation_date
    from most_recent_session
    where rn = 1 

    UNION ALL

    select
        dependent_id,
        session_id,
        date_of_service,
        NULL as evaluation_summary_from_session,
        NULL as treatment_plan_recommendation_from_session,
        NULL as evaluation_recommendation_from_session,
        NULL as goals_long_term_timeline_from_session,
        'most recent evaluation' as source,
        evaluation_summary as evaluation_summary_from_evaluation,
        treatment_plan_recommendations as treatment_plan_recommendation_from_evaluation,
        recommendation as evaluation_recommendation,
        GOALS_LONG_TERM_TIMELINE as goals_long_term_timeline,
        NULL as session_date,
        date_of_service as evaluation_date
    from most_recent_evaluation
    where rn = 1
)

select
    dependent_id,
    max(case when source = 'most recent session' then session_id end) as most_recent_session_id,
    max(case when source = 'most recent evaluation' then session_id end) as most_recent_evaluation_id,
    max(session_date) as most_recent_session_date,
    max(evaluation_date) as most_recent_evaluation_date,
    max(evaluation_summary_from_session) as evaluation_summary_from_session,
    max(treatment_plan_recommendation_from_session) as treatment_plan_recommendation_from_session,
    max(evaluation_recommendation_from_session) as evaluation_recommendation_from_session,
    max(goals_long_term_timeline_from_session) as goals_long_term_timeline_from_session,
    max(evaluation_summary_from_evaluation) as evaluation_summary_from_evaluation,
    max(treatment_plan_recommendation_from_evaluation) as treatment_plan_recommendation_from_evaluation,
    max(evaluation_recommendation) as evaluation_recommendation,
    max(goals_long_term_timeline) as goals_long_term_timeline
from combined_data
group by dependent_id

