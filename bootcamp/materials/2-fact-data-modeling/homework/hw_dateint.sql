
WITH users AS(
	SELECT *
	FROM user_devices_cumulated
	WHERE date = '2023-01-31'
),
	series_date AS(
	SELECT *
	FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') AS sequence_date
),
	int_datelist_placeholder AS (
	SELECT 	
		CASE WHEN device_activity_date_list @> ARRAY[DATE(sequence_date)]
			THEN POW(2, 32 - (date - DATE(sequence_date)))
		ELSE 0 END AS datelist_int
		,*
	FROM users CROSS JOIN series_date
)

SELECT
	user_id,
	device_id,
	browser_type,
	CAST(CAST(SUM(datelist_int) AS BIGINT) AS BIT(32)) AS active_date
FROM int_datelist_placeholder
GROUP BY user_id, device_id, browser_type

-- monthy active

SELECT
	user_id,
	device_id,
	browser_type,
	CAST(CAST(SUM(datelist_int) AS BIGINT) AS BIT(32)) AS active_date,
	BIT_COUNT(CAST(CAST(SUM(datelist_int) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active
FROM int_datelist_placeholder
GROUP BY user_id, device_id, browser_type

