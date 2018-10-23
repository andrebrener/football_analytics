from constants_xg import (
    FIXED_DUMMIES, ID_COLS, SET_PIECE_STANDARDS, TARGET_COL
)


def score_ranges(n):
    if n > 8:
        return 'more_than_8'
    else:
        return str(n)


def time_ranges(n):
    if n < 15:
        return 'first_fifteen'
    elif n < 30:
        return 'second_fifteen'
    elif n < 45:
        return 'third_fifteen'
    else:
        return 'injury_time'


def shot_number_ranges(n):
    if n < 4:
        return 'very_few'
    elif n < 7:
        return 'few'
    elif n < 10:
        return 'many'
    else:
        return 'too_many'


def possession_time_ranges(n):
    if n < 1:
        return 'very_short'
    elif n < 5:
        return 'short'
    elif n < 12:
        return 'long'
    else:
        return 'very_long'


def pos_velocity_x_ranges(n):
    val = abs(n)
    if val < 2:
        return 'very_slow'
    elif val < 5:
        return 'slow'
    elif val < 27:
        return 'fast'
    else:
        return 'very_fast'


def pos_velocity_y_ranges(n):
    val = abs(n)
    if val < 2:
        return 'very_slow'
    elif val < 7:
        return 'slow'
    elif val < 30:
        return 'fast'
    else:
        return 'very_fast'


def cons_passes_in_pos_done_ranges(n):
    if n < 1:
        return 'no_pass'
    elif n < 3:
        return 'few'
    elif n < 5:
        return 'many'
    else:
        return 'too_many'


def distance_from_prev_event_ranges(n):
    if n < 10:
        return 'very_short'
    elif n < 17:
        return 'short'
    elif n < 28:
        return 'far'
    else:
        return 'very_far'


def y_ranges(n):
    if n < 14:
        return 'wide_right'
    elif n < 28:
        return 'right'
    elif n < 42:
        return 'center'
    elif n < 56:
        return 'left'
    else:
        return 'wide_left'


def x_ranges(n):
    if n > 100:
        return 'very_close'
    elif n > 95:
        return 'close'
    elif n > 85:
        return 'mid_range'
    else:
        return 'long_range'


def build_ranges(df):

    df = df.copy()
    # Columns with ranges
    range_cols = {}

    range_cols['cons_passes_in_pos_done'] = cons_passes_in_pos_done_ranges
    range_cols['possession_time'] = possession_time_ranges
    range_cols['game_minutes'] = time_ranges
    range_cols['len'] = distance_from_prev_event_ranges

    for c in ['pos_dest_x', 'pos_x']:
        range_cols[c] = x_ranges

    for c in ['pos_dest_y', 'pos_y']:
        range_cols[c] = y_ranges

    for _ in ['own', 'rival']:
        for c, f in [
            ('score', score_ranges), ('shot_number', shot_number_ranges)
        ]:
            range_cols['{}_{}'.format(_, c)] = f

    for col, f in [('x', pos_velocity_x_ranges), ('y', pos_velocity_y_ranges)]:
        range_cols['pos_{}_velocity'.format(col)] = f

    for c, f in range_cols.items():
        df['{}_range'.format(c)] = df[c].apply(lambda x: f(x))

    return df


def dummize_cols(df):
    cols = []
    for col, vals in FIXED_DUMMIES.items():
        for v in vals:
            col_name = '{}_{}'.format(col, v)
            df[col_name] = df[col].apply(lambda x: 1 if str(x) == v else 0)
            cols.append(col_name)

    return df.copy(), cols


def build_X(df):

    # Get only shots
    shots_df = df[df['is_shot'] > 0].copy()

    # Remove penalty & free kicks. All set pieces that ended in a shot
    open_play_shots_df = shots_df[~shots_df['play_standard_id'].
                                  isin(SET_PIECE_STANDARDS)]

    processed_df = build_ranges(open_play_shots_df)

    final_df, model_cols = dummize_cols(processed_df)

    total_cols = TARGET_COL + ID_COLS + model_cols

    train_df = final_df[total_cols]

    validation_df = final_df[total_cols]

    pred_data_df = df

    return pred_data_df, train_df, validation_df, model_cols


if __name__ == '__main__':

    pred_data_df, train_df, validation_df, model_cols = build_X()
    import pdb; pdb.set_trace()  # noqa # yapf: disable
