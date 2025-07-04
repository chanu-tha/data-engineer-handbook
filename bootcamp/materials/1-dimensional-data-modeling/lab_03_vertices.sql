
SELECT * FROM games
LIMIT 50;


-- information of games
INSERT INTO vertices
SELECT
	game_id as identifier,
	'game'::vertex_type as type,
	json_build_object(
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
	) as properties

FROM games;

-- all time player stats
INSERT INTO vertices
WITH player_agg AS(
SELECT
	player_id as identifier,
	MAX(player_name) as player_name,
	COUNT(1) as number_of_game_played,
	SUM(pts) as total_points,
	ARRAY_AGG(DISTINCT team_id) as teams
FROM game_details
GROUP BY player_id
)

SELECT
	identifier,
	'player'::vertex_type as type,
	json_build_object(
		'player_name', player_name,
		'number_of_game_played', number_of_game_played,
		'total_points', total_points,
		'teams', teams
	)
FROM player_agg


-- team properties
INSERT INTO vertices
WITH teams_deduped as(
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY team_id) as row_num
	FROM teams
) 

SELECT 
	team_id as identifier,
	'team'::vertex_type as type,
	json_build_object(
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena,
		'year_founded', yearfounded
	)
FROM teams_deduped
WHERE row_num = 1;



SELECT DISTINCT(abbreviation)
FROM teams

