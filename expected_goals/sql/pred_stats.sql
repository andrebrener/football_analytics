CREATE TABLE pred_stats (
 id bigserial PRIMARY KEY,
 game_id integer REFERENCES games_info(game_id),
 sequence_number integer,
 xg decimal,
 xa decimal,
 pos_xg_chain decimal,
 pos_xg_buildup decimal
 );

