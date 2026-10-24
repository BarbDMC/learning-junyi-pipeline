WITH source AS (
    SELECT * FROM {{ source('raw_junyi', 'info_user_data') }}
)

SELECT
    uuid AS user_id,
    NULLIF(gender, '') AS gender,
    NULLIF(user_grade, '')::INTEGER AS user_grade,
    NULLIF(user_city, '') AS user_city,
    
    -- Profile and gamification metrics
    COALESCE(NULLIF(points, '')::BIGINT, 0) AS total_points,
    COALESCE(NULLIF(badges_cnt, '')::INTEGER, 0) AS total_badges,
    
    -- Boolean indicators
    CASE 
        WHEN NULLIF(has_teacher_cnt, '')::INTEGER > 0 THEN TRUE 
        ELSE FALSE 
    END AS has_teacher,
    
    CASE 
        WHEN LOWER(is_self_coach) = 'true' THEN TRUE 
        ELSE FALSE 
    END AS is_self_coach,

    -- Dates
    NULLIF(first_login_date_tw, '')::DATE AS first_login_date

FROM source
WHERE uuid IS NOT NULL
