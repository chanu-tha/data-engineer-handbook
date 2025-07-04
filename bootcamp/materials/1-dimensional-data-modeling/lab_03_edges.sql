
SELECT 
	v.properties->>'player_name' AS player_name,
	MAX(CAST(e.properties->>'pts' AS INTEGER)) AS max_point
FROM vertices v JOIN edges e
	ON e.subject_identifier = v.identifier
	AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC


INSERT INTO edges
WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
	filtered AS(
		SELECT * 
		FROM deduped
		WHERE row_num = 1
),
-- perform self join
	aggregate_single_edge AS (
		SELECT 
			f1.player_id AS subject_player_id,
			f2.player_id AS object_player_id,
			CASE WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
				ELSE 'play_against'::edge_type
				END AS edge_type,
			COUNT(1) AS num_game,
			MAX(f1.player_name) AS subject_player_name,
			MAX(f2.player_name) AS object_player_name,
			SUM(f1.pts) AS subject_points,
			SUM(f2.pts) AS object_points
		FROM filtered f1 JOIN filtered f2
			ON f1.game_id = f2.game_id
			AND f1.player_name <> f2.player_name
			WHERE f1.player_id > f2.player_id
			GROUP BY f1.player_id,
					f2.player_id,
					edge_type
)

SELECT 
	subject_player_id AS identifier,
	'player'::vertex_type AS subject_type,
	object_player_id AS object_identifier,
	'player'::vertex_type AS object_type,
	edge_type,
	json_build_object(
		'num_game', num_game,
		'subject_points', subject_points,
		'object_points', object_points
	)
FROM aggregate_single_edge;

SELECT
    player_id AS subject_identifier,
    'player'::vertex_type AS subject_type,
    game_id AS object_identifier,
    'game'::vertex_type AS object_type,
    'play_in'::edge_type AS edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
        ) as properties
FROM deduped
WHERE row_num = 1;

SELECT
	v.properties->>'player_name',
	e.object_identifier,
	CAST(v.properties->>'number_of_games')
FROM vertices v JOIN edge e










