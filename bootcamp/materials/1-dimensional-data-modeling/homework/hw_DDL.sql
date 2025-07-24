SELECT * FROM actor_films
WHERE actor = 'Jerry Lewis'

CREATE TYPE properties AS(
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
)

CREATE TYPE quality_class AS
	ENUM('star', 'good', 'average', 'bad');


DROP TABLE actors

CREATE TABLE actors(
	actor TEXT,
	actorid TEXT,
	films properties[],
	quality_class quality_class,
	is_active BOOLEAN,
	current_year INTEGER,
	PRIMARY KEY(actor, actorid, current_year)
)

DROP TABLE actor_history_scd

CREATE TABLE actor_history_scd(
	actor TEXT,
	actorid TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER,
	current_year INTEGER,
	PRIMARY KEY(actorid, start_date)
)
	