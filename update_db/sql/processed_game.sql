select * from
    (
    select * from game_events
    where game_id={game_id}
    ) ge
    left join
    (
    select player_id, in_first_11
    from players_stats
    ) ps
    on ge.player_id = ps.player_id
    left join
    (
    select id, foot_id as player_foot_id
    from players_info
    ) pi
    on ge.player_id = pi.id
    order by sequence_number
