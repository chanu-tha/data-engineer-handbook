CREATE TYPE vertex_type
	AS ENUM('player', 'team', 'game');

CREATE TABLE vertices(
	identifier TEXT,
	type vertex_type,
	properties JSON,
	PRIMARY KEY(identifier, type)

);

CREATE TYPE edge_type AS
	ENUM('play_against', 
			'shares_team',
			'play_in' --play in game,
			'play_on' -- play on team
);


CREATE TABLE edges(
	subject_identifier TEXT,
	subject_type vertex_type,
	object_identifier TEXT,
	object_type vertex_type,
	edge_type edge_type,
	properties JSON,
	PRIMARY KEY(subject_identifier,
				subject_type,
				object_identifier,
				object_type,
				edge_type)
;



			