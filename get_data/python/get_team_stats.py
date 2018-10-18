import os
import sys
import json
import logging
import logging.config

import pandas as pd

from api_calls import get_api_call
from constants import GAME_INFO_URL, PROJECT_DIR, TEAMS_STATS_COLS, TEAMS_TPL
from db_handle import insert_values

sys.path.append(PROJECT_DIR)

from config import config

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


# conversion function:
def dict_to_json(dictionary):
    return json.dumps(dictionary, ensure_ascii=False)


def get_teams_stats(game_id):

    json_data = get_api_call(GAME_INFO_URL.format(TEAMS_TPL, game_id))

    teams = json_data['data']['team']

    teams_dfs = []
    for t in teams:
        logger.info("Getting data for {}".format(t['name']))
        stats = t['param']

        stats_dict = {}
        other_stats = {}

        all_stats = {
            s['name'].lower().replace('/ ', '').replace('- ', '').replace(
                ',', ''
            ).replace('.', '').replace(' ', '_').replace('-', '_'): s['value']
            for s in stats
        }

        import pdb; pdb.set_trace()  # noqa # yapf: disable

        for k, v in TEAMS_STATS_COLS.items():
            if k in all_stats.keys():
                stats_dict[k] = [all_stats[k]]
            else:
                stats_dict[k] = [0]

        for k, v in all_stats.items():
            if k not in TEAMS_STATS_COLS.keys():
                other_stats[k] = v

        stats_dict['other_stats'] = [other_stats]

        team_df = pd.DataFrame(stats_dict)
        team_df['team_id'] = t['id']
        team_df['game_id'] = game_id

        no_format_cols = ['other_stats']

        for c in [c for c in team_df.columns if c not in no_format_cols]:
            team_df[c] = team_df[c].astype(float)

        teams_dfs.append(team_df)

    teams_df = pd.concat(teams_dfs, sort=True)
    teams_df['other_stats'] = teams_df['other_stats'].map(dict_to_json)

    return teams_df


def insert_teams_stats(game_id):
    teams_df = get_teams_stats(game_id)
    insert_values(teams_df, 'teams_stats', TEAMS_STATS_COLS)
    logger.info("Inserted teams stats for game {}".format(game_id))
    return None


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])

    game_id = '1234769'

    teams_df, query = insert_teams_stats(game_id)

    import pdb; pdb.set_trace()  # noqa # yapf: disable
