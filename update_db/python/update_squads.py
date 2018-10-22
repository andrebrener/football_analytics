import os
import sys
import logging
import logging.config

from datetime import datetime

import pandas as pd

from constants import (
    GET_DATA_DIR, LIB_COMMON_DIR, PROJECT_DIR, SQUAD_UPDATE_DICT
)

for d in [PROJECT_DIR, LIB_COMMON_DIR, GET_DATA_DIR]:
    sys.path.append(d)

from config import config
from api_calls import get_api_call
from get_squads import get_players_info, insert_df
from constants_gd import PLAYERS_INFO_COLS, TEAMS_INFO_TPL, TEAMS_INFO_URL

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def get_teams_info():

    total_teams_dfs = []

    for l, s in SQUAD_UPDATE_DICT.items():
        logger.info("Getting teams for tournament {}".format(l))
        json_data = get_api_call(TEAMS_INFO_URL.format(TEAMS_INFO_TPL, l, s))

        teams = json_data['data']['row']

        for t in teams:
            team_dict = {k: [v] for k, v in t.items()}

            team_df = pd.DataFrame(team_dict)

            total_teams_dfs.append(team_df)

    teams_df = pd.concat(total_teams_dfs, sort=True)

    teams_df['created_on'] = datetime.now()

    return teams_df


def insert_teams_players():
    teams_df = get_teams_info()
    players_df = get_players_info(teams_df)

    teams_stats_dict = {c: c for c in teams_df.columns}
    for df, table, stats_dict in [
        (teams_df, 'teams_info', teams_stats_dict),
        (players_df, 'players_info', PLAYERS_INFO_COLS)
    ]:
        insert_df(df, table, stats_dict)

    logger.info("Inserted teams and players")
    return None


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])

    insert_teams_players()

    import pdb; pdb.set_trace()  # noqa # yapf: disable
