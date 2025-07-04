-- SELECT * FROM player_seasons
-- LIMIT 100;

-- CREATE TYPE season_stats AS (
-- 						season INTEGER,
-- 						gp INTEGER,
-- 						pts REAL,
-- 						reb REAL,
-- 						ast REAL
-- )

-- CREATE TABLE for unchange value
-- CREATE TABLE players (
-- 	player_name TEXT,
-- 	height TEXT,
-- 	college TEXT,
-- 	country TEXT,
-- 	draft_year TEXT,
-- 	draft_round TEXT,
-- 	draft_number TEXT,
-- 	season_stats season_stats[],
-- 	current_season INTEGER,
-- 	PRIMARY KEY(player_name, current_season)
-- )


-- INSERT INTO players
-- WITH yesterday AS (
-- 	SELECT * FROM players
-- 	WHERE current_season = 1995
-- ),
-- today AS (
-- 	SELECT * FROM player_seasons
-- 	WHERE season = 1996
-- )
-- SELECT 
-- 	COALESCE(t.player_name, y.player_name) as player_name,
-- 	COALESCE(t.height, y.height) as height,
-- 	COALESCE(t.college, y.college) as college,
-- 	COALESCE(t.country, y.country) as country,
-- 	COALESCE(t.draft_year, y.draft_year) as draft_year,
-- 	COALESCE(t.draft_round, y.draft_round) as draft_round,
-- 	COALESCE(t.draft_number, y.draft_number) as draft_number,
-- 	CASE WHEN y.season_stats IS NULL
-- 		THEN ARRAY[ROW(
-- 			t.season,
-- 			t.gp,
-- 			t.pts,
-- 			t.reb,
-- 			t.ast)::season_stats]
-- 		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
-- 			t.season,
-- 			t.gp,
-- 			t.pts,
-- 			t.reb,
-- 			t.ast)::season_stats]
-- 		ELSE y.season_stats
-- 	END as season_stats,
-- 	COALESCE(t.season, y.current_season + 1) as current_season
	
-- FROM today t
-- FULL OUTER JOIN yesterday y
-- ON t.player_name = y.player_name;


-- SELECT count(1) FROM player_seasons
-- WHERE season = 1996;

-- UPDATE the stats of current season into players
INSERT INTO players
WITH yesterday AS (
	SELECT * FROM players
	WHERE current_season = 2000
),
today AS (
	SELECT * FROM player_seasons
	WHERE season = 2001
)
SELECT 
	COALESCE(t.player_name, y.player_name) as player_name,
	COALESCE(t.height, y.height) as height,
	COALESCE(t.college, y.college) as college,
	COALESCE(t.country, y.country) as country,
	COALESCE(t.draft_year, y.draft_year) as draft_year,
	COALESCE(t.draft_round, y.draft_round) as draft_round,
	COALESCE(t.draft_number, y.draft_number) as draft_number,
	CASE WHEN y.season_stats IS NULL
		THEN ARRAY[ROW(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast)::season_stats]
		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast)::season_stats]
		ELSE y.season_stats
	END as season_stats,
	COALESCE(t.season, y.current_season + 1) as current_season
	
FROM today t
FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name;


SELECT * FROM players WHERE player_name = 'Michael Jordan' and current_season = 2001;

-- de-construct the struct
SELECT player_name,
	UNNEST(season_stats) as season_stats
FROM players
WHERE player_name = 'Michael Jordan' 
	AND current_season = 2001;


-- de-construct in to columns
WITH unnested as(
SELECT player_name,
	UNNEST(season_stats)::season_stats as season_stats
FROM players
WHERE player_name = 'Michael Jordan' 
	AND current_season = 2001
)

SELECT player_name,
	(season_stats::season_stats).*
FROM unnested


-- CREATE score_class to lable player performance
--CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');


-- CREATE TABLE players (
-- 	player_name TEXT,
-- 	height TEXT,
-- 	college TEXT,
-- 	country TEXT,
-- 	draft_year TEXT,
-- 	draft_round TEXT,
-- 	draft_number TEXT,
-- 	season_stats season_stats[],
-- 	scoring_class scoring_class,
-- 	years_since_last_season INTEGER,
-- 	current_season INTEGER,
-- 	PRIMARY KEY(player_name, current_season)
-- )

INSERT INTO players
WITH yesterday AS (
	SELECT * FROM players
	WHERE current_season = 2000
),
today AS (
	SELECT * FROM player_seasons
	WHERE season = 2001
)
SELECT 
	COALESCE(t.player_name, y.player_name) as player_name,
	COALESCE(t.height, y.height) as height,
	COALESCE(t.college, y.college) as college,
	COALESCE(t.country, y.country) as country,
	COALESCE(t.draft_year, y.draft_year) as draft_year,
	COALESCE(t.draft_round, y.draft_round) as draft_round,
	COALESCE(t.draft_number, y.draft_number) as draft_number,
	CASE WHEN y.season_stats IS NULL
		THEN ARRAY[ROW(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast)::season_stats]
		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
			t.season,
			t.gp,
			t.pts,
			t.reb,
			t.ast)::season_stats]
		ELSE y.season_stats
	END as season_stats,
	CASE WHEN t.season IS NOT NULL THEN
		CASE WHEN t.pts > 20 THEN 'star'
			WHEN t.pts > 15 THEN 'good'
			WHEN t.pts > 10 THEN 'average'
			ELSE 'bad'
		END::scoring_class
		ELSE y.scoring_class
	END AS scoring_class,
	CASE WHEN t.season IS NOT NULL THEN 0
		ELSE y.years_since_last_season + 1
	END as years_since_last_season,
	
	COALESCE(t.season, y.current_season + 1) as current_season
	
FROM today t
FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name;


-- analytics

SELECT 
	player_name,
	(season_stats[1]::season_stats).pts as first_season,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts as latest_season
FROM players
WHERE current_season = 2001;


SELECT 
	player_name, 
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts/
	CASE WHEN (season_stats[1]::season_stats).pts = 0 THEN 1 ELSE (season_stats[1]::season_stats).pts END AS pct_improvement
	
FROM players
WHERE current_season = 2001
ORDER BY 2 DESC;










