DO $$
DECLARE
   -- Declare variables to hold the start and end years for processing.
   start_year INT;
   end_year INT;
   -- The 'loop_year' variable for the loop is declared implicitly.
BEGIN
   -- Step 1: Clear the target table to ensure a fresh start.
   RAISE NOTICE 'Clearing the "actors" table...';
   TRUNCATE TABLE actors;
   RAISE NOTICE '"actors" table cleared successfully.';

   -- Step 2: Dynamically determine the range of years from the source data.
   RAISE NOTICE 'Determining year range from actor_films table...';
   SELECT MIN(year), MAX(year) INTO start_year, end_year FROM actor_films;

   IF start_year IS NULL THEN
      RAISE NOTICE 'Warning: "actor_films" table appears to be empty. No data to process.';
      RETURN;
   END IF;

   RAISE NOTICE 'Processing data from year % to %', start_year, end_year;

   -- Step 3: Loop through each year, from the earliest to the latest.
   FOR loop_year IN start_year..end_year LOOP
      RAISE NOTICE '  -> Processing year % (based on last year %)', loop_year, loop_year - 1;

      -- Step 4: Execute the main INSERT logic for the current year in the loop.
      -- Note: The column list in the INSERT statement is a good practice.
      INSERT INTO actors
      WITH this_year AS (
          SELECT *
          FROM actor_films
          WHERE year = loop_year -- Use the current year from the loop
      ),
      last_year AS (
          SELECT *
          FROM actors
          WHERE current_year = loop_year - 1 -- Use the previous year
      ),
      agg_film AS (
          SELECT
              actor,
              actorid,
              year AS agg_year,
              ARRAY_AGG(ROW(film, 
			  				votes, 
							rating,
							filmid)::properties) AS films,
              ROUND(AVG(rating)::NUMERIC, 2) AS rating
          FROM this_year
          GROUP BY actor, actorid, year
      )
      SELECT
          COALESCE(agg.actor, ly.actor) AS actor,
		  COALESCE(agg.actorid, ly.actorid) AS actorid,
          CASE
              WHEN ly.films IS NULL THEN agg.films
			  WHEN ly.films IS NOT NULL THEN ly.films || agg.films
              ELSE ly.films
          END AS films,
          CASE
              WHEN agg.rating IS NOT NULL THEN (
                  CAST(
                      CASE
                          WHEN agg.rating > 8 THEN 'star'
                          WHEN agg.rating > 7 THEN 'good'
                          WHEN agg.rating > 6 THEN 'average'
                          ELSE 'bad'
                      END
                  AS quality_class) -- Explicitly cast to your custom type
              )
              ELSE ly.quality_class
          END AS quality_class,
          CASE
              WHEN agg.films IS NOT NULL THEN TRUE
              ELSE FALSE
          END AS is_active,
          COALESCE(agg.agg_year, ly.current_year + 1) AS current_year
      FROM agg_film agg
      FULL OUTER JOIN last_year ly ON agg.actorid = ly.actorid;

   END LOOP;

   RAISE NOTICE 'Successfully populated the "actors" table.';

END $$;