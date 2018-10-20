import os
import sys
import logging
import logging.config

from datetime import datetime

import pandas as pd

from constants import COLS_TO_INSERT, PROJECT_DIR
from db_handle import insert_values

sys.path.append(PROJECT_DIR)

from config import config

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')

os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def get_shot_possessions(df):

    # Identify possessions with shots
    grouped_pos = df[['game_id', 'possession_number', 'is_shot']].groupby(
        ['game_id', 'possession_number']
    ).sum().reset_index()
    grouped_pos.columns = ['game_id', 'possession_number', 'shots_in_pos']

    merged_df = pd.merge(df, grouped_pos, how='left')

    # Get only possessions that include a shot
    shot_pos_df = merged_df[merged_df['shots_in_pos'] > 0].copy()

    return shot_pos_df


def get_xa(df):
    if df['is_pass'] > 0 and df['game_id'] == df['next_game_id']:
        return df['next_xg']
    else:
        return 0


def get_xg_buildup(df):
    if df['is_shot'] > 0:
        return 0
    elif df['next_is_shot'] > 0 and df['game_id'] == df['next_game_id']:
        return 0
    else:
        return df['pos_xg_chain']


def get_predicted_values(df):

    for c in ['xg', 'game_id', 'possession_number']:
        df['next_{}'.format(c)] = df[c].shift(-1)

    # Add xA
    df['xa'] = df.apply(lambda x: get_xa(x), axis=1)

    # Add xG Chain
    xg_pos = df[['game_id', 'possession_number',
                 'xg']].groupby(['game_id',
                                 'possession_number']).max().reset_index()
    xg_pos.columns = ['game_id', 'possession_number', 'pos_xg_chain']

    merged_df = pd.merge(df, xg_pos, how='left')

    # Add xG Buildup
    merged_df['next_is_shot'] = merged_df['is_shot'].shift(-1)
    merged_df['pos_xg_buildup'] = merged_df.apply(
        lambda x: get_xg_buildup(x), axis=1
    )

    final_df = merged_df[COLS_TO_INSERT]

    return final_df


def insert_predictive_stats(df):
    shot_pos_df = get_shot_possessions(df)
    pred_df = get_predicted_values(shot_pos_df)
    # Insert in postgres
    stats_dict = {c: c for c in pred_df.columns}
    insert_values(pred_df, 'pred_stats', stats_dict)
    logger.info("Predictive values inserted")


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])
    t0 = datetime.now()
    insert_predictive_stats()
    t1 = datetime.now()
    logger.info("The process took {}".format(t1 - t0))
