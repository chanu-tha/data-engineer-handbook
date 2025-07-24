
-- CREATE TABLE host_activity_reduced(
-- 	month_start DATE,
-- 	host TEXT,
-- 	hits_array REAL[],
-- 	unique_visitors_array REAL[],
-- 	PRIMARY KEY(host, month_start)
-- )

--TRUNCATE TABLE host_activity_reduced
--SELECT * FROM host_activity_reduced

INSERT INTO host_activity_reduced
WITH today_agg AS (
	SELECT 
		DATE(event_time) AS date,
		host,
		COUNT(1) AS hits,
		COUNT(DISTINCT user_id) AS user_cnt
	FROM events
	WHERE DATE(event_time) = '2023-01-31'
	GROUP BY host, DATE(event_time)
),
	yesterday AS(
	SELECT *
	FROM host_activity_reduced
	-- date will always be start of the month
	WHERE month_start = '2023-01-01'
)

SELECT 
	COALESCE(y.month_start, DATE_TRUNC('month', agg.date)) AS month_start,
	COALESCE(agg.host, y.host) AS host,
	-- append new array to the end
	CASE WHEN y.hits_array IS NOT NULL THEN y.hits_array || ARRAY[COALESCE(agg.hits,0)]
		WHEN y.hits_array IS NULL THEN 
		ARRAY_FILL(0, ARRAY[COALESCE(agg.date - DATE(DATE_TRUNC('month', agg.date)), 0)]) || ARRAY[COALESCE(agg.hits,0)]
	END AS hits_array,
	
	CASE WHEN y.unique_visitors_array IS NOT NULL THEN y.unique_visitors_array || ARRAY[COALESCE(agg.user_cnt,0)]
		WHEN y.unique_visitors_array IS NULL THEN 
		ARRAY_FILL(0, ARRAY[COALESCE(agg.date - DATE(DATE_TRUNC('month', agg.date)), 0)]) || ARRAY[COALESCE(agg.user_cnt,0)]
	END AS unique_visitors_array

FROM today_agg agg
FULL OUTER JOIN yesterday y
	ON agg.host = y.host

ON CONFLICT (month_start, host)
DO
	UPDATE SET hits_array = EXCLUDED.hits_array,
				unique_visitors_array = EXCLUDED.unique_visitors_array


