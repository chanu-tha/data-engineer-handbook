
INSERT INTO actor_history_scd
WITH with_previous AS (
	SELECT
		actor,
		actorid,
		quality_class,
		is_active,
		current_year,
		LAG(quality_class, 1) OVER( PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
		LAG(is_active, 1) OVER( PARTITION BY actorid ORDER BY current_year) AS previous_is_active
	
	FROM actors
	WHERE current_year <= 2020
),
	indicators AS(
		SELECT *,
			CASE WHEN quality_class <> previous_quality_class THEN 1
				WHEN is_active <> previous_is_active THEN 1
			ELSE 0 END AS change_indicator
		FROM with_previous
),
	with_streaks AS(
		SELECT *,
			SUM(change_indicator) OVER(PARTITION BY actorid ORDER BY current_year) AS streak_identifier
		FROM indicators
)

SELECT
	actor,
	actorid,
	quality_class,
	is_active,
	MIN(current_year) AS start_date,
	MAX(current_year) AS end_date,
	2020 AS current_year
FROM with_streaks
GROUP BY actor, actorid, streak_identifier, quality_class, is_active
ORDER BY actor, streak_identifier

