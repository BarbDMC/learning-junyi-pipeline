WITH problems AS (
    SELECT * FROM {{ ref('stg_log_problem') }}
)

SELECT
    -- Foreign surrogate keys (star schema joins)
    MD5(user_id) AS user_sk,
    MD5(content_id) AS content_sk,
    TO_CHAR(interaction_date, 'YYYYMMDD')::INTEGER AS date_sk,
    interaction_hour AS time_sk,

    -- Degenerate dimensions (event-level transaction IDs)
    problem_instance_id,
    problem_number,

    -- Effort and resolution metrics
    time_taken_seconds,
    attempts_count,
    hints_count,
    is_correct,
    is_hint_used,
    
    -- Derived metric: solved on the first attempt without hints
    CASE 
        WHEN is_correct IS TRUE AND attempts_count = 1 AND hints_count = 0 THEN TRUE 
        ELSE FALSE 
    END AS is_first_attempt_correct

FROM problems