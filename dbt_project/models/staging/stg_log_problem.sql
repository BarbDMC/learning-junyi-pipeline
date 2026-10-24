WITH source AS (
    SELECT * FROM {{ source('raw_junyi', 'log_problem') }}
)

SELECT
    -- Identifiers
    uuid AS user_id,
    ucid AS content_id,
    NULLIF(upid, '') AS problem_instance_id,
    NULLIF(problem_number, '')::INTEGER AS problem_number,
    
    -- Timestamps
    NULLIF(timestamp_tw, '')::TIMESTAMP AS interaction_timestamp,
    (NULLIF(timestamp_tw, '')::TIMESTAMP)::DATE AS interaction_date,
    EXTRACT(HOUR FROM NULLIF(timestamp_tw, '')::TIMESTAMP)::INTEGER AS interaction_hour,

    -- Resolution metrics
    COALESCE(NULLIF(total_sec_taken, '')::NUMERIC, 0) AS time_taken_seconds,
    COALESCE(NULLIF(total_attempt_cnt, '')::INTEGER, 1) AS attempts_count,
    COALESCE(NULLIF(used_hint_cnt, '')::INTEGER, 0) AS hints_count,

    -- Success and behavior flags
    CASE 
        WHEN LOWER(is_correct) = 'true' THEN TRUE 
        ELSE FALSE 
    END AS is_correct,
    
    CASE 
        WHEN LOWER(is_hint_used) = 'true' THEN TRUE 
        ELSE FALSE 
    END AS is_hint_used

FROM source
WHERE uuid IS NOT NULL 
  AND ucid IS NOT NULL
