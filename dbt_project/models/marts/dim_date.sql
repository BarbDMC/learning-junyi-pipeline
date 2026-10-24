WITH date_series AS (
    SELECT 
        generate_series(
            '2015-01-01'::DATE, 
            '2025-12-31'::DATE, 
            '1 day'::INTERVAL
        )::DATE AS date_day
)

SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_sk,
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day)::INTEGER AS year,
    EXTRACT(QUARTER FROM date_day)::INTEGER AS quarter,
    EXTRACT(MONTH FROM date_day)::INTEGER AS month,
    TO_CHAR(date_day, 'TMMonth') AS month_name,
    EXTRACT(DAY FROM date_day)::INTEGER AS day_of_month,
    EXTRACT(ISODOW FROM date_day)::INTEGER AS day_of_week,
    TO_CHAR(date_day, 'TMDay') AS day_name,
    CASE 
        WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
FROM date_series
