-- Incremental query

-- CREATE TYPE actor_scd_type AS(
-- 	quality_class quality_class,
-- 	is_active BOOLEAN,
-- 	start_date INTEGER,
-- 	end_date INTEGER
-- )


WITH last_year_scd AS(
	SELECT * 
	FROM actor_history_scd
	WHERE current_year = 2020
	AND end_date = 2020
),
	historical_actor_records AS(
	SELECT actor,
			actorid,
			quality_class,
			is_active,
			start_date,
			end_date
	FROM actor_history_scd
	WHERE current_year =2020
	AND end_date < 2020
),
	this_year AS (
	SELECT *
	FROM actors
	WHERE current_year = 2021
),
	unchanged_records AS (
	SELECT ty.actor,
			ty.actorid,
			ty.quality_class,
			ty.is_active,
			ls.start_date,
			ty.current_year AS end_date
	FROM this_year ty
	JOIN last_year_scd ls
		ON ty.actorid = ls.actorid
	WHERE ty.quality_class = ls.quality_class
		AND ty.is_active = ls.is_active
),
	changed_records AS(
		SELECT ty.actor,
				ty.actorid,
				UNNEST(ARRAY[
					-- previous records
					ROW(
						ls.quality_class,
						ls.is_active,
						ls.start_date,
						ls.end_date
					)::actor_scd_type,
					--current year records
					ROW(
						ty.quality_class,
						ty.is_active,
						ty.current_year,
						ty.current_year
					)::actor_scd_type
				])as records
				
		FROM last_year_scd ls
		LEFT JOIN this_year ty
			ON ls.actorid = ty.actorid
		WHERE ty.quality_class <> ls.quality_class
		OR ty.is_active <> ls.is_active
		
),
	unnested_changed_records AS(
		SELECT actor,
				actorid,
				(records::actor_scd_type).quality_class,
				(records::actor_scd_type).is_active,
				(records::actor_scd_type).start_date,
				(records::actor_scd_type).end_date
		FROM changed_records
),

	new_records AS(
		SELECT ty.actor,
				ty.actorid,
				ty.quality_class,
				ty.is_active,
				ty.current_year AS start_date,
				ty.current_year AS end_date
		FROM this_year ty
		LEFT JOIN last_year_scd ls
			ON ty.actorid = ls.actorid
		WHERE ls.actorid IS NULL
	)


SELECT *,
	2021 AS current_year
FROM (
	SELECT * FROM historical_actor_records

	UNION ALL

	SELECT * FROM unchanged_records

	UNION ALL

	SELECT * FROM unnested_changed_records

	UNION ALL

	SELECT * FROM new_records
)
ORDER BY actor, actorid, start_date