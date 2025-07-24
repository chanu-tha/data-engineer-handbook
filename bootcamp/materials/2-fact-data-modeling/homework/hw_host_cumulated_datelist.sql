-- DROP TABLE hosts_cumulated

-- CREATE TABLE hosts_cumulated (
-- 	host TEXT,
-- 	activity_date DATE[],
-- 	date DATE,
-- 	PRIMARY KEY(host, date)
-- )
-- TRUNCATE TABLE hosts_cumulated

INSERT INTO hosts_cumulated
WITH yesterday AS(
	SELECT * 
	FROM hosts_cumulated
	WHERE date = '2023-01-30'
),
today AS(
	SELECT host,
		DATE(event_time) AS date
	FROM events
	WHERE DATE(event_time) = '2023-01-31'
	GROUP BY host, DATE(event_time)
)

SELECT 
	COALESCE(t.host, y.host) AS host,
	CASE WHEN y.activity_date IS NULL THEN ARRAY[t.date]
		WHEN t.date IS NULL THEN y.activity_date
	ELSE y.activity_date || ARRAY[t.date] END AS activity_date,
	COALESCE(t.date, y.date + INTERVAL '1 DAY') AS date
FROM today t
FULL OUTER JOIN yesterday y
	ON t.host = y.host 


-- SELECT * FROM hosts_cumulated

