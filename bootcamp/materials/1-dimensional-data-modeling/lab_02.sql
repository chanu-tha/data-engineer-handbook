select * from players 
insert into players 
with years as (
	select *
	from generate_series(1996, 2022) as season
),
p as (
	select player_name , MIN(season) as first_season 
	from player_seasons 
	group by player_name 
),
players_and_seasons as (
	select * 
	from p
	join years y 
	on p.first_season <= y.season
),
windowed as (
	select 
	ps.player_name, ps.season,
	array_remove(
	array_agg(case 
		when p1.season is not null then 
		cast(row(p1.season, p1.gp, p1.pts, p1.reb, p1.ast) as season_stats)
		end
		)
	over (partition by ps.player_name order by coalesce(p1.season, ps.season)) 
	,null
) 
as seasons
	from players_and_seasons ps
	left join player_seasons p1
	on ps.player_name = p1.player_name and ps.season = p1.season
	order by ps.player_name, ps.season
)
,static as ( 
	select player_name,
	max(height) as height,
	max(college) as college,
	max(country) as country,
	max(draft_year) as draft_year,
	max(draft_round) as draft_round,
	max(draft_number) as draft_number
	from player_seasons ps 
	group by player_name
	)
	
select 
	w.player_name, 
	s.height,
	s.college,
	s.country,
	s.draft_year,
	s.draft_number,
	s.draft_round,
	seasons as season_stats
--	,( seasons[cardinality(seasons)]).pts
	,case 
	when (seasons[cardinality(seasons)]).pts > 20 then 'star'
	when (seasons[cardinality(seasons)]).pts > 15 then 'good'
	when (seasons[cardinality(seasons)]).pts > 10 then 'average'
	else 'bad'
	end :: scoring_class as scorring_class
	,w.season - (seasons[cardinality(seasons)]).season as years_since_last_season
	,w.season as current_season
	,(seasons[cardinality(seasons)]).season = w.season as is_active
from windowed w 
join static s
on w.player_name = s.player_name;




-- CREATE TYPE scd_type AS(
-- 					scoring_class scoring_class,
-- 					is_active BOOLEAN,
-- 					start_season INTEGER,
-- 					end_season INTEGER
-- )

WITH last_season_scd AS(
	
		SELECT *
		FROM player_scd
		WHERE current_season = 2021
		AND end_season = 2021
),
	historical_data AS(

		SELECT player_name,
				scoring_class,
				is_active,
				start_season,
				end_season
		FROM player_scd
		WHERE current_season = 2021
),
	this_season AS(

		SELECT * 
		FROM players_auto
		WHERE current_season = 2022
), 
	unchanged_records AS(

		SELECT ts.player_name,
				ts.scoring_class,
				ts.is_active,
				ls.start_season,
				ts.current_season as end_season
		FROM this_season ts
		JOIN last_season_scd ls
		ON ts.player_name = ls.player_name
		WHERE ts.scoring_class = ls.scoring_class
		AND ts.is_active = ls.is_active
),
	changed_records AS(

		SELECT ts.player_name,
				UNNEST(ARRAY[
					--put the old record in here
					ROW(
						ls.scoring_class,
						ls.is_active,
						ls.start_season,
						ls.end_season
					)::scd_type,
					-- new changed record of current season
					ROW(
						ts.scoring_class,
						ts.is_active,
						ts.current_season,
						ts.current_season
					)::scd_type
				]) as records
		FROM this_season ts
		LEFT JOIN last_season_scd ls
		ON ts.player_name = ls.player_name
		WHERE (ts.scoring_class <> ls.scoring_class
		OR ts.is_active <> ls.is_active)
),
	unnested_changed_records AS (
		--flatten the struct
		SELECT player_name,
			(records::scd_type).scoring_class,
			(records::scd_type).is_active,
			(records::scd_type).start_season,
			(records::scd_type).end_season
		
		FROM changed_records
),
	
	new_records AS (
		SELECT ts.player_name,
				ts.scoring_class,
				ts.is_active,
				ts.current_season as start_season,
				ts.current_season as end_season
		
		FROM this_season ts
		LEFT JOIN last_season_scd ls
		ON ts.player_name = ls.player_name
		WHERE ls.player_name IS NULL
	)

SELECT *,2022 AS current_season FROM (SELECT * FROM historical_data

UNION ALL

SELECT * FROM unchanged_records

UNION ALL

SELECT * FROM unnested_changed_records

UNION ALL	

SELECT * FROM new_records) a

