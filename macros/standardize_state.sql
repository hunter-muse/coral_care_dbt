{% macro standardize_state(state_input) %}
    CASE
        -- Convert full names to abbreviations (case insensitive)
        WHEN LOWER({{ state_input }}) = 'alabama' THEN 'AL'
        WHEN LOWER({{ state_input }}) = 'alaska' THEN 'AK'
        WHEN LOWER({{ state_input }}) = 'arizona' THEN 'AZ'
        WHEN LOWER({{ state_input }}) = 'arkansas' THEN 'AR'
        WHEN LOWER({{ state_input }}) = 'california' THEN 'CA'
        WHEN LOWER({{ state_input }}) = 'colorado' THEN 'CO'
        WHEN LOWER({{ state_input }}) = 'connecticut' THEN 'CT'
        WHEN LOWER({{ state_input }}) = 'delaware' THEN 'DE'
        WHEN LOWER({{ state_input }}) = 'district of columbia' THEN 'DC'
        WHEN LOWER({{ state_input }}) = 'florida' THEN 'FL'
        WHEN LOWER({{ state_input }}) = 'georgia' THEN 'GA'
        WHEN LOWER({{ state_input }}) = 'hawaii' THEN 'HI'
        WHEN LOWER({{ state_input }}) = 'idaho' THEN 'ID'
        WHEN LOWER({{ state_input }}) = 'illinois' THEN 'IL'
        WHEN LOWER({{ state_input }}) = 'indiana' THEN 'IN'
        WHEN LOWER({{ state_input }}) = 'iowa' THEN 'IA'
        WHEN LOWER({{ state_input }}) = 'kansas' THEN 'KS'
        WHEN LOWER({{ state_input }}) = 'kentucky' THEN 'KY'
        WHEN LOWER({{ state_input }}) = 'louisiana' THEN 'LA'
        WHEN LOWER({{ state_input }}) = 'maine' THEN 'ME'
        WHEN LOWER({{ state_input }}) = 'maryland' THEN 'MD'
        WHEN LOWER({{ state_input }}) = 'massachusetts' THEN 'MA'
        WHEN LOWER({{ state_input }}) = 'michigan' THEN 'MI'
        WHEN LOWER({{ state_input }}) = 'minnesota' THEN 'MN'
        WHEN LOWER({{ state_input }}) = 'mississippi' THEN 'MS'
        WHEN LOWER({{ state_input }}) = 'missouri' THEN 'MO'
        WHEN LOWER({{ state_input }}) = 'montana' THEN 'MT'
        WHEN LOWER({{ state_input }}) = 'nebraska' THEN 'NE'
        WHEN LOWER({{ state_input }}) = 'nevada' THEN 'NV'
        WHEN LOWER({{ state_input }}) = 'new hampshire' THEN 'NH'
        WHEN LOWER({{ state_input }}) = 'new jersey' THEN 'NJ'
        WHEN LOWER({{ state_input }}) = 'new mexico' THEN 'NM'
        WHEN LOWER({{ state_input }}) = 'new york' THEN 'NY'
        WHEN LOWER({{ state_input }}) = 'north carolina' THEN 'NC'
        WHEN LOWER({{ state_input }}) = 'north dakota' THEN 'ND'
        WHEN LOWER({{ state_input }}) = 'ohio' THEN 'OH'
        WHEN LOWER({{ state_input }}) = 'oklahoma' THEN 'OK'
        WHEN LOWER({{ state_input }}) = 'oregon' THEN 'OR'
        WHEN LOWER({{ state_input }}) = 'pennsylvania' THEN 'PA'
        WHEN LOWER({{ state_input }}) = 'rhode island' THEN 'RI'
        WHEN LOWER({{ state_input }}) = 'south carolina' THEN 'SC'
        WHEN LOWER({{ state_input }}) = 'south dakota' THEN 'SD'
        WHEN LOWER({{ state_input }}) = 'tennessee' THEN 'TN'
        WHEN LOWER({{ state_input }}) = 'texas' THEN 'TX'
        WHEN LOWER({{ state_input }}) = 'utah' THEN 'UT'
        WHEN LOWER({{ state_input }}) = 'vermont' THEN 'VT'
        WHEN LOWER({{ state_input }}) = 'virginia' THEN 'VA'
        WHEN LOWER({{ state_input }}) = 'washington' THEN 'WA'
        WHEN LOWER({{ state_input }}) = 'west virginia' THEN 'WV'
        WHEN LOWER({{ state_input }}) = 'wisconsin' THEN 'WI'
        WHEN LOWER({{ state_input }}) = 'wyoming' THEN 'WY'
        
        -- If already a valid 2-letter code, return as is (after upper-casing)
        WHEN LENGTH(TRIM({{ state_input }})) = 2 AND 
             UPPER({{ state_input }}) IN ('AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY',
                                    'LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH',
                                    'OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY') 
            THEN UPPER({{ state_input }})
            
        -- Return NULL for invalid values
        ELSE NULL
    END
{% endmacro %}