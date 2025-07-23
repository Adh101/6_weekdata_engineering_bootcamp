WITH user_devices AS(
		SELECT * FROM user_devices_cumulated
		WHERE date = '2023-01-31'
),
series AS(
		SELECT * FROM generate_series(DATE('2023-01-01'),DATE('2023-01-31'), INTERVAL '1 day') AS series_date
),
-- Coverting the dates to binary
placeholder_ints AS(
					SELECT 
                        CASE WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
                                THEN CAST(POW(2,32-(date - DATE(series_date))) AS BIGINT)
                            ELSE 0
                        END AS datelist_int
                        ,*
					FROM user_devices
					CROSS JOIN series
)

SELECT 
	user_id,
	browser_type,
    --Compress activity into a 32-bit bitmap where each bit maps to a date in Jan 2023
	CAST(CAST(SUM(datelist_int) AS BIGINT) AS BIT(32)) AS datelist_int	
FROM placeholder_ints
GROUP BY user_id, browser_type;