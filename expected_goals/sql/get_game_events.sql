select * from
    (
    select * from game_events
    ) ge
    inner join
    (
    select code, name as action_name from actions
    )a
    on ge.action_id=a.code
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
    where game_id in
    (
    select game_id from games_info
    where date >= '{sd}'
    and date < '{ed}'
    )
    order by sequence_number
