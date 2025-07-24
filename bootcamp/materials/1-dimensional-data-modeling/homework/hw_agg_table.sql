-- actor_films is source
-- create agg before create cumulative table


INSERT INTO actors
WITH this_year AS(
	SELECT * FROM actor_films
	WHERE year = '1971'
),
	last_year AS(
	SELECT * FROM actors
	WHERE current_year = '1970'
	),
	agg_film AS(
	SELECT
		actor,
		actorid,
		year AS agg_year,
		ARRAY_AGG(ROW(
					film,
					votes,
					rating,
					filmid
		)::properties) AS films,
		ROUND(AVG(rating)::NUMERIC,2) AS rating
FROM this_year
GROUP BY actor, actorid, year
)

SELECT COALESCE(agg.actor, ly.actor) AS actor,
		COALESCE(agg.actorid, ly.actorid) AS actorid,
		CASE WHEN ly.films IS NULL THEN agg.films
			WHEN ly.films IS NOT NULL THEN ly.films || agg.films
		ELSE ly.films END AS films,
		
		CASE WHEN agg.rating IS NOT NULL THEN( 
			CASE WHEN agg.rating > 8 THEN 'star'
				WHEN agg.rating > 7 THEN 'good'
				WHEN agg.rating > 6 THEN 'average'
			ELSE 'bad' END)::quality_class
		ELSE ly.quality_class
		END AS quality_class,
		
		
		CASE WHEN agg.films IS NULL THEN FALSE 
		ELSE TRUE END AS is_active,
		COALESCE(agg.agg_year,ly.current_year + 1) AS current_year		

FROM agg_film agg
	FULL OUTER JOIN last_year ly
	ON agg.actorid = ly.actorid


TRUNCATE TABLE actors

SELECT * FROM actors
ORDER BY actor
	
