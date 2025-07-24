-- DROP TABLE user_devices_cumulated

-- CREATE TABLE user_devices_cumulated(
-- 	user_id TEXT,
-- 	device_id TEXT,
-- 	browser_type TEXT,
-- 	device_activity_date_list DATE[],
-- 	date DATE,
-- 	PRIMARY KEY(user_id, device_id, browser_type, date)
-- )

--TRUNCATE TABLE user_devices_cumulated

-- start_date 2023-01-01
-- end_date 2023-01-31


INSERT INTO user_devices_cumulated
WITH yesterday AS(
	SELECT *
	FROM user_devices_cumulated
	WHERE date = '2023-01-30'
),
	device_w_event AS(
		SELECT e.user_id,
			e.device_id,
			d.browser_type,
			e.event_time,
			ROW_NUMBER() OVER(PARTITION BY e.user_id, e.event_time) AS row_num
		FROM events e
		JOIN devices d
			ON e.device_id = d.device_id
		WHERE e.user_id IS NOT NULL
),
	device_w_event_deduped AS(
		SELECT *
		FROM device_w_event
		WHERE row_num = 1
),
	today AS(
	SELECT CAST(user_id AS text) AS user_id,
		CAST(device_id AS text) AS device_id,
		browser_type,
		DATE(CAST(event_time AS TIMESTAMP)) AS date_active
	FROM device_w_event_deduped
	WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
		AND user_id IS NOT NULL
	GROUP BY user_id, device_id, browser_type, DATE(CAST(event_time AS TIMESTAMP))
)

SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(t.device_id, y.device_id) AS device_id,
	COALESCE(t.browser_type, y.browser_type) AS browser_type,
	CASE WHEN y.device_activity_date_list IS NULL THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.device_activity_date_list
		ELSE ARRAY[t.date_active] || y.device_activity_date_list 
		END AS date_active,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t
	FULL OUTER JOIN yesterday y
	ON t.user_id = y.user_id AND t.device_id = y.device_id


	