WITH hours AS (
    SELECT generate_series(0, 23) AS hour_24
)

SELECT
    hour_24 AS time_sk,
    hour_24,
    TO_CHAR(TO_TIMESTAMP(hour_24::TEXT, 'HH24'), 'HH12 AM') AS hour_12,
    CASE 
        WHEN hour_24 < 12 THEN 'AM' 
        ELSE 'PM' 
    END AS am_pm,
    CASE 
        WHEN hour_24 BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour_24 BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN hour_24 BETWEEN 18 AND 23 THEN 'Evening'
        ELSE 'Night'
    END AS day_part
FROM hours
