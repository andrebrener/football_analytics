CREATE TABLE players_info (
 id integer PRIMARY KEY,
 firstname VARCHAR (355) NOT NULL,
 lastname VARCHAR (355) NOT NULL,
 birthday timestamp,
 country_id integer,
 position2_id integer,
 position3_id integer,
 foot_id integer REFERENCES body_parts(id),
 height decimal,
 weight decimal,
 created_on TIMESTAMP NOT NULL
 );
