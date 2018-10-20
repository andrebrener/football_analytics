from constants import (
    ASSIST_DICT, GOAL_IDS, OWN_GOAL_IDS, POS_START_IDS, RED_CARD_IDS,
    REMOVE_CODES, SET_PIECE_STANDARDS
)


def assign_team(df, col_suffix):
    own_cols = []
    rival_cols = []

    assign_df = df.copy()

    for l in ['home', 'away']:
        col_name = '{}_{}'.format(l, col_suffix)
        own_col = 'own_{}_{}'.format(col_suffix, l)
        rival_col = 'rival_{}_{}'.format(col_suffix, l)

        assign_df[own_col] = assign_df.apply(
            lambda x: x[col_name] if x['home_away'] == l else 0, axis=1
        )

        assign_df[rival_col] = assign_df.apply(
            lambda x: x[col_name] if x['home_away'] != l else 0, axis=1
        )

        own_cols.append(own_col)
        rival_cols.append(rival_col)

    assign_df['own_{}'.format(col_suffix)] = assign_df[own_cols].sum(axis=1)
    assign_df['rival_{}'.format(col_suffix)
              ] = assign_df[rival_cols].sum(axis=1)

    return assign_df


def get_goals(df):

    df['goal'] = df['action_id'].apply(lambda x: 1 if x in GOAL_IDS else 0)

    # Add own goals
    df['own_goal'] = df['action_id'].apply(
        lambda x: 1 if x in OWN_GOAL_IDS else 0
    )

    for l in ['home', 'away']:
        df['{}_regular_goal'.format(l)] = df.apply(
            lambda x: x['goal'] if x['home_away'] == l else 0, axis=1
        )

        df['{}_favour_own_goal'.format(l)] = df.apply(
            lambda x: x['own_goal'] if x['home_away'] != l else 0, axis=1
        )

        df['{}_total_goals'.format(l)
           ] = df['{}_regular_goal'.format(l)
                  ] + df['{}_favour_own_goal'.format(l)]

        df['{}_score'.format(l)] = df['{}_total_goals'.format(l)].cumsum()

    # Assign score
    final_df = assign_team(df, 'score')

    return final_df


def identify_red_cards(df, l):
    if df['action_id'] in RED_CARD_IDS and df['home_away'] == l:
        return -1
    else:
        return 0


def get_players_in_field(df):

    # Create a column for each team with a -1 in the row of every red card
    for l in ['home', 'away']:
        rc_col = '{}_red_card'.format(l)

        # Insert red cards
        df[rc_col] = df.apply(lambda x: identify_red_cards(x, l), axis=1)

        # Make a cumsum for red cards

        cum_col = '{}_cum_rc'.format(l)

        df[cum_col] = df[rc_col].cumsum()

        # Subtract the cumulative expulsions from the 11 initial players
        df['{}_players'.format(l)] = 11 + df[cum_col]

    final_df = assign_team(df, 'players')

    return final_df


def get_shot_number(df):

    # Create a column for each team with a 1 in the row of every shot
    for l in ['home', 'away']:
        shot_col = '{}_shots'.format(l)
        df[shot_col] = df.apply(
            lambda x: x['is_shot'] if x['home_away'] == l else 0, axis=1
        )

        # Make a cumsum for shots
        cum_col = '{}_shot_number'.format(l)
        df[cum_col] = df[shot_col].cumsum()

    final_df = assign_team(df, 'shot_number')

    return final_df


def get_passes_in_possession(df):

    # Get consecutive rows in possession
    col_name = 'cons_passes_in_pos'

    df[col_name] = df.groupby('possession_number')['is_pass'].cumsum()

    df[col_name] = df[col_name].apply(lambda x: x if x > 0 else 0)

    df['cons_passes_in_pos_done'] = df[col_name].shift()

    return df


def get_formations(df):
    for l in ['home', 'away']:
        col_name = '{}_formation'.format(l)
        df[col_name] = df.apply(
            lambda x: x['formation'] if x['home_away'] == l else None, axis=1
        )
        df[col_name] = df[col_name].fillna(method='ffill')

    df['own_formation'] = df.apply(
        lambda x: x['home_formation'] if x['home_away'] == 'home' else x['away_formation'],
        axis=1
    )

    df['rival_formation'] = df.apply(
        lambda x: x['home_formation'] if x['home_away'] == 'away' else x['away_formation'],
        axis=1
    )

    return df


def get_pos_starts(df, c):
    if df['possession_id'] in POS_START_IDS:
        if df['is_shot'] > 0 and df['play_standard_id'
                                    ] not in SET_PIECE_STANDARDS:
            return df['prev_{}'.format(c)]
        else:
            return df[c]
    else:
        return None


def get_possession_start(df):
    df['minsec'] = df['game_minutes'] * 60 + df['game_seconds']

    for c in ['minsec', 'pos_x', 'pos_y']:
        df['prev_{}'.format(c)] = df[c].shift()
        df['pos_started_{}'.format(c)] = df.apply(
            lambda x: get_pos_starts(x, c), axis=1
        ).fillna(method='ffill')

    df['possession_time'] = df['minsec'] - df['pos_started_minsec']

    for coord in ['x', 'y']:
        col = 'pos_{}'.format(coord)
        df['{}_velocity'.format(col)
           ] = (df[col] - df['pos_started_{}'.format(col)
                             ]) / df['possession_time']

    return df


def get_prev_event(df):
    for c in ['action_name', 'action_id', 'body_id', 'len', 'direction']:
        df[c] = df[c].fillna(0)
        df['prev_{}'.format(c)] = df[c].shift()

    return df


def get_clean_df(df):
    clean_df = df[df['possession_number'].notnull()]
    clean_df = clean_df[~clean_df['action_id'].isin(REMOVE_CODES)]
    clean_df['action_id'] = clean_df['action_id'].apply(
        lambda x: ASSIST_DICT[x] if x in ASSIST_DICT.keys() else x
    )

    return clean_df


def process_game(df):

    clean_df = get_clean_df(df)
    goals_df = get_goals(clean_df)
    cards_df = get_players_in_field(goals_df)
    shot_number_df = get_shot_number(cards_df)
    passes_df = get_passes_in_possession(shot_number_df)
    formations_df = get_formations(passes_df)
    pos_start_df = get_possession_start(formations_df)
    prev_event_df = get_prev_event(pos_start_df)

    final_df = prev_event_df
    return final_df


if __name__ == '__main__':
    from db_handle import get_df_from_query

    game_id = '1234772'
    query = """
    select * from
    (
    select * from game_events
    where game_id={}
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
    order by sequence_number
    """.format(game_id)

    df = get_df_from_query(query)

    test = process_game(df)

    import pdb; pdb.set_trace()  # noqa # yapf: disable
