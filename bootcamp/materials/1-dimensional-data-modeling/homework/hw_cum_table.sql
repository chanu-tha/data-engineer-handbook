
-- MIN(year): 1970
-- MAX(year): 2021

SELECT * FROM actor_films

WITH this_year AS(
	SELECT * FROM actor_films
	WHERE year = '1971'
),
	last_year AS(
	SELECT * FROM actors
	WHERE current_year = '1970'
	)

SELECT * 
FROM this_year t
	FULL OUTER JOIN last_year l
	ON t.actor = l.actor



/*
SELECT 
	COALESCE(l.actor, t.actor) AS actor,
	CASE WHEN l.films IS NULL	
		THEN ARRAY[ROW(
				t.film,
				t.votes,
				t.rating,
				t.filmid
				)::properties]
		WHEN t.film IS NOT NULL THEN l.films || ARRAY[ROW(
				t.film,
				t.votes,
				t.rating,
				t.filmid
				)::properties] 
		ELSE l.films END AS films,
	
	CASE WHEN l.quality_class IS NULL THEN(
		CASE WHEN AVG(t.rating) > 8 THEN 'star'
			WHEN AVG(t.rating) > 7 THEN 'good'
			WHEN AVG(t.rating) > 6 THEN 'average'
			ELSE 'bad' END)::quality_class
		ELSE l.quality_class END 
	AS quality_class,
	
	t.year IS NOT NULL is_active,
	COALESCE(t.year, l.current_year + 1) AS current_year
FROM this_year t
	FULL OUTER JOIN last_year l
	ON t.actor = l.actor
*/


