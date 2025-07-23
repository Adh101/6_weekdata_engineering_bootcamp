--A cumulative query to generate device_activity_datelist from events
-- Appends the active date for (user_id, browser_type) and deduplicates it daily
INSERT INTO user_devices_cumulated
WITH yesterday AS(
		SELECT *
		FROM user_devices_cumulated
		WHERE date = DATE('2023-01-30')
),
today AS(
		SELECT CAST(e.user_id AS text) AS user_id,
				CAST(d.browser_type AS text) AS browser_type,
				DATE(CAST(e.event_time AS TIMESTAMP)) AS date_active	
		FROM events  AS e
		JOIN devices  AS d
		    ON e.device_id = d.device_id
		WHERE DATE(CAST(e.event_time AS TIMESTAMP)) = DATE('2023-01-31')
		    AND e.user_id IS NOT NULL
		GROUP BY e.user_id, d.browser_type, DATE(CAST(e.event_time AS TIMESTAMP))
)
SELECT 
		COALESCE(t.user_id,y.user_id) AS user_id,
		COALESCE(t.browser_type, y.browser_type) AS browser_type,
        -- If yesterday’s array is NULL, start with today’s date
        -- Else append today’s date and remove duplicates
		CASE
			WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
			WHEN t.date_active IS NULL THEN y.device_activity_datelist
	        ELSE (
	            SELECT ARRAY(
	                SELECT DISTINCT unnest(y.device_activity_datelist || t.date_active)
	                ORDER BY 1 DESC
	            )
	        )
			END AS device_activity_datelist,
		COALESCE(t.date_active,y.date + INTERVAL '1 day') AS date		
FROM today  AS t
--To ensure we capture new users/devices (only in today) or old ones with no new activity (only in yesterday) using FULL OUTER JOIN
FULL OUTER JOIN yesterday AS y
    ON t.user_id = y.user_id AND t.browser_type = y.browser_type;