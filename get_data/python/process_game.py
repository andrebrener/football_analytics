import pandas as pd

from constants import (
    ACTION_CODES, FORMATION_CODES, GAME_EVENTS_COLS, INITIAL_CODES,
    POSITION_CODES, REMOVE_CODES
)


def get_pls_sum_in_formation(df):
    if df['is_change_formation'] > 0:
        return sum(
            [int(x) for x in df['action_name'].split()[0].split('-')] + [1]
        )
    else:
        return 0


def get_formation_json(df):
    if df['is_change_formation'] > 0:
        form_dict = {}
        for n in range(1, df['pls_in_formation'] + 1):
            key = list(df['player_{}'.format(n)].keys())[0]
            val = df['player_{}'.format(n)][key]
            form_dict[key] = val
        return form_dict
    else:
        return None


def get_formation(df):
    df['player_dict'] = df.apply(
            lambda x: {x['action_name']: {x['player_id']: x['player_name']}}, axis=1)

    for n in range(1, 12):
        df['player_{}'.format(n)] = df['player_dict'].shift(-n)

    df['players_formation'] = df.apply(lambda x: get_formation_json(x), axis=1)

    return df


def get_change_formation(df):
    if df['second'] == 0:
        return 0
    elif df['is_formation'] > 0:
        return 1
    else:
        return 0


def clean_formations(df):

    df['is_formation'] = df['action_id'].apply(
        lambda x: 1 if x in FORMATION_CODES else 0
    )

    # df['pls_in_formation'] = df.apply(
    # lambda x: get_pls_sum_in_formation(x), axis=1
    # )

    # form_df = get_formation(df)

    df['formation'] = df.apply(
        lambda x: x['action_name'].split()[0] if x['is_formation'] > 0 else None,
        axis=1
    )

    clean_df = df[~df['action_id'].isin(POSITION_CODES)].copy()

    clean_df['is_change_formation'] = clean_df.apply(
        lambda x: get_change_formation(x), axis=1
    )

    return clean_df


def add_rival(df):
    teams = df['team_id'].dropna().unique().tolist()
    rival_df = df.copy()

    rival_df['rival_team_id_dropna'] = rival_df['team_id'].apply(
        lambda x: teams[0] if x == teams[1] else teams[1]
    )

    rival_df['false_team_id'] = rival_df['team_id'].fillna(0)

    rival_df['rival_team_id'] = rival_df.apply(
        lambda x: x['rival_team_id_dropna'] if x['false_team_id'] != 0 else None,
        axis=1
    )

    return rival_df


def get_original_time(df, half_start):
    if df['second'] - half_start > 0:
        return df['second'] - half_start
    elif df['action_id'] in INITIAL_CODES:
        return 0
    else:
        return -1


def correct_time(df):
    game_list = []
    for h, half_df in df.groupby('half'):
        # Get the time when he first possession in the half starts
        # Start pos id = 1
        half_df = half_df.copy()

        half_start = float(
            half_df[half_df['possession_id'] == 1]['second'].iloc[0]
        )

        half_df['game_total_seconds'] = half_df.apply(
            lambda x: get_original_time(x, half_start), axis=1
        )
        half_df['game_minutes'] = (half_df['game_total_seconds'] /
                                   60).astype(int)
        half_df['game_seconds'] = half_df['game_total_seconds'
                                          ] - half_df['game_minutes'] * 60

        game_list.append(half_df)

    game_df = pd.concat(
        game_list, sort=True
    ).sort_values(
        [
            'half', 'game_minutes', 'game_seconds', 'possession_number',
            'action_name'
        ]
    )

    return game_df


def remove_unused_actions(df):
    clean_df = df[~df['action_id'].isin(REMOVE_CODES)].copy()

    # Add sequence number
    final_df = clean_df.reset_index().reset_index()
    final_df.rename(columns={'level_0': 'sequence_number'}, inplace=True)

    return final_df


def correct_types(df):

    no_format_cols = ['formation', 'home_away']

    df['second'] = df['second'].fillna(0).astype(float)

    cols_to_correct = [c for c in df.columns if c in GAME_EVENTS_COLS.keys()]

    for c in [c for c in cols_to_correct if c not in no_format_cols]:
        df[c] = df[c].astype(float)

    return df


def get_clean_df(df):
    clean_df = remove_unused_actions(df)
    final_df = correct_types(clean_df)

    return final_df


def get_action_cols(df):
    for a, codes in ACTION_CODES.items():
        df['is_{}'.format(a)
           ] = df['action_id'].apply(lambda x: 1 if x in codes else 0)

    return df


def process_game(df):

    clean_df = get_clean_df(df)
    time_df = correct_time(clean_df)
    df_with_formations = clean_formations(time_df)
    action_df = get_action_cols(df_with_formations)
    final_df = add_rival(action_df)

    sel_cols = list(GAME_EVENTS_COLS.keys())

    return final_df[sel_cols]


if __name__ == '__main__':
    import os

    from constants import PYTHON_DIR

    p = os.path.join(PYTHON_DIR, 'talleres_belgrano.csv')
    df = pd.read_csv(p)

    test = process_game(df)

    import pdb; pdb.set_trace()  # noqa # yapf: disable
