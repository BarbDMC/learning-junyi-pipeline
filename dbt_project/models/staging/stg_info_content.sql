WITH source AS (
    SELECT * FROM {{ source('raw_junyi', 'info_content') }}
)

SELECT
    ucid AS content_id,
    NULLIF(content_pretty_name, '') AS content_name,
    NULLIF(content_kind, '') AS content_kind,
    NULLIF(difficulty, '') AS difficulty,
    NULLIF(subject, '') AS subject_area,
    NULLIF(learning_stage, '') AS learning_stage,
    
    -- Curriculum taxonomy
    NULLIF(level1_id, '') AS level1_id,
    NULLIF(level2_id, '') AS level2_id,
    NULLIF(level3_id, '') AS level3_id,
    NULLIF(level4_id, '') AS level4_id

FROM source
WHERE ucid IS NOT NULL
