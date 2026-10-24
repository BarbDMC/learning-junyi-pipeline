WITH source AS (
    SELECT * FROM {{ ref('stg_info_content') }}
)

SELECT
    MD5(content_id) AS content_sk,
    content_id,
    content_name,
    content_kind,
    difficulty,
    subject_area,
    learning_stage,
    level1_id,
    level2_id,
    level3_id,
    level4_id
FROM source
