import os
import sys
import logging
import logging.config

import pandas as pd

from api_calls import get_api_call
from constants import (
    GAME_INFO_URL, PLAYERS_STATS_COLS, PLAYERS_TPL, PROJECT_DIR
)
from db_handle import insert_values
from get_team_stats import dict_to_json

sys.path.append(PROJECT_DIR)

from config import config

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def get_players_stats(game_id):

    json_data = get_api_call(GAME_INFO_URL.format(PLAYERS_TPL, game_id))

    teams = json_data['data']['team']

    players_dfs = []
    for t in teams:
        logger.info("Getting players for {}".format(t['name']))
        for pl in t['player']:
            stats = pl['param']
            stats_dict = {}
            other_stats = {}

            all_stats = {
                s['name'].lower().replace('/ ', '').replace('- ', '').replace(
                    ',', ''
                ).replace('.', '').replace(' ', '_').replace('-', '_'):
                s['value']
                for s in stats
            }
            for k, v in PLAYERS_STATS_COLS.items():
                if k in all_stats.keys():
                    stats_dict[k] = [all_stats[k]]
                else:
                    stats_dict[k] = [0]

            for k, v in all_stats.items():
                if k not in PLAYERS_STATS_COLS.keys():
                    other_stats[k] = v

            stats_dict['other_stats'] = [other_stats]

            player_df = pd.DataFrame(stats_dict)
            player_df['player_id'] = pl['id']
            player_df['team_id'] = t['id']
            player_df['game_id'] = game_id

            no_format_cols = ['other_stats']

            for c in [c for c in player_df.columns if c not in no_format_cols]:
                player_df[c] = player_df[c].astype(float)

            players_dfs.append(player_df)

    players_df = pd.concat(players_dfs, sort=True)
    players_df['other_stats'] = players_df['other_stats'].map(dict_to_json)

    return players_df


def insert_players_stats(game_id):
    players_df = get_players_stats(game_id)
    insert_values(players_df, 'players_stats', PLAYERS_STATS_COLS)
    logger.info("Inserted players stats for game {}".format(game_id))

    return None


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])

    game_id = '1234771'

    players_df = insert_players_stats(game_id)

    import pdb; pdb.set_trace()  # noqa # yapf: disable
