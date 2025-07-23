--An incremental query that loads host_activity_reduced
-- Appends new daily values into monthly arrays for hits and unique visitors
INSERT INTO host_activity_reduced
WITH daily_aggregated AS(
			SELECT host,
					DATE(event_time) AS date,
					COUNT(1) as hits,
					COUNT(DISTINCT user_id) AS unique_visitors		
			FROM events
			WHERE DATE(event_time) = DATE('2023-01-03')
			AND user_id IS NOT NULL
			GROUP BY host, DATE(event_time)
),
yesterday AS (
    SELECT *
    FROM host_activity_reduced
    WHERE month_start = DATE('2023-01-01')
)

SELECT
    COALESCE(d.host, y.host) AS host,
    COALESCE(y.month_start, DATE_TRUNC('month',d.date)) AS month_start,
    CASE
        WHEN y.hit_array IS NULL THEN ARRAY[d.hits]
        ELSE y.hit_array || d.hits
    END AS hit_array,
    CASE
        WHEN y.unique_visitors_array IS NULL THEN ARRAY[d.unique_visitors]
        ELSE y.unique_visitors_array || d.unique_visitors
    END AS unique_visitors_array
FROM daily_aggregated  AS d
FULL OUTER JOIN yesterday  AS y
    ON d.host = y.host
ON CONFLICT (host, month_start)
DO UPDATE SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array;