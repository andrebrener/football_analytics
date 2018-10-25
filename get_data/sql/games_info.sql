CREATE TABLE games_info (
 id serial PRIMARY KEY,
 game_id integer UNIQUE NOT NULL,
 tournament_id integer NOT NULL,
 season_id integer REFERENCES seasons(id),
 round_id integer NOT NULL REFERENCES rounds(id),
 home_team_id integer REFERENCES teams_info(id),
 away_team_id integer REFERENCES teams_info(id),
 status_id integer NOT NULL REFERENCES games_status(id),
 home_team_score integer NOT NULL,
 away_team_score integer NOT NULL,
 stadium_id integer NOT NULL,
 duration DECIMAL,
 date TIMESTAMP NOT NULL
 );
