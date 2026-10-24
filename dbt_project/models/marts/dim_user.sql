WITH source AS (
    SELECT * FROM {{ ref('stg_info_user_data') }}
)

SELECT
    MD5(user_id) AS user_sk,
    user_id,
    gender,
    user_grade,
    user_city,
    total_points,
    total_badges,
    has_teacher,
    is_self_coach,
    first_login_date
FROM source
