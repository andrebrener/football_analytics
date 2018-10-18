CREATE TABLE game_events (
 id bigserial PRIMARY KEY,
 game_id integer REFERENCES games_info(game_id),
 team_id integer REFERENCES teams_info(id),
 possession_team_id integer REFERENCES teams_info(id),
 rival_team_id integer REFERENCES teams_info(id),
 player_id integer REFERENCES players_info(id),
 second_player_id integer REFERENCES players_info(id),
 action_id integer REFERENCES actions(code),
 attack_status_id integer REFERENCES attack_status(id),
 attack_type_id integer REFERENCES attack_types(id),
 body_id integer REFERENCES body_parts(id),
 position_id integer REFERENCES positions(id),
 possession_id integer REFERENCES possession_types(id),
 play_standard_id integer REFERENCES standards(id),
 direction decimal,
 half smallint,
 len decimal,
 pos_dest_x decimal,
 pos_dest_y decimal,
 pos_x decimal,
 pos_y decimal,
 possession_number integer,
 video_seconds decimal,
 home_away VARCHAR (355),
 game_minutes integer,
 game_seconds decimal,
 sequence_number integer,
 formation VARCHAR (355),
 is_pass smallint,
 is_shot smallint,
 is_change_formation smallint
 );

