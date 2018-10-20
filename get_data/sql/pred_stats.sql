CREATE TABLE pred_stats (
 id bigserial PRIMARY KEY,
 game_id integer REFERENCES game_events(game_id),
 sequence_number integer REFERENCES game_events(sequence_number),
 xg decimal,
 xa decimal,
 pos_xg_chain decimal,
 pos_xg_buildup decimal
 );

