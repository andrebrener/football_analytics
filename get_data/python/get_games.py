import os
import sys
import logging
import logging.config

import pandas as pd

from api_calls import get_api_call
from constants import (
    GAME_INFO_COLS, GAMES_ID_URL, GAMES_TPL, LEAGUES, PROJECT_DIR, SEASONS
)
from db_handle import insert_values

sys.path.append(PROJECT_DIR)

from config import config

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def get_games_id(sd, ed):

    games = {}

    logger.info("Getting data for games between {} - {}".format(sd, ed))
    total_games_dfs = []
    for l in LEAGUES:
        for s in SEASONS:
            json_data = get_api_call(
                GAMES_ID_URL.format(GAMES_TPL, l, s, sd, ed)
            )
            games = json_data['data']['row']

            for g in games:
                game_dict = {}
                for key, val in g.items():
                    if key in GAME_INFO_COLS.keys():
                        game_dict[key] = [val]

                game_df = pd.DataFrame(game_dict)

                total_games_dfs.append(game_df)

    games_df = pd.concat(total_games_dfs, sort=True)

    return games_df


def insert_game_info(sd, ed):
    games_df = get_games_id(sd, ed)
    insert_values(games_df, 'games_info', GAME_INFO_COLS)
    logger.info("Inserted game info for games between {} - {}".format(sd, ed))
    return games_df


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])

    from datetime import date

    sd = date(2018, 10, 6)
    ed = date(2018, 10, 9)

    games_df = insert_game_info(sd, ed)

    import pdb; pdb.set_trace()  # noqa # yapf: disable
