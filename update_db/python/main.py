import os
import sys
import logging
import logging.config

from datetime import datetime

from constants import (
    END_DATE, GET_DATA_DIR, LEAGUES, LIB_COMMON_DIR, PROJECT_DIR, SEASONS,
    START_DATE
)

for d in [GET_DATA_DIR, PROJECT_DIR, LIB_COMMON_DIR]:
    sys.path.append(d)

from config import config
from db_handle import get_df_from_query
from get_games import insert_game_info
from main_get_data import get_all_game_info

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def get_processed_game(game_id):
    sql_path = os.path.join(PROJECT_DIR, 'sql', 'processed_game.sql')
    query = open(sql_path, 'r').read().format(game_id=game_id)
    df = get_df_from_query(query)
    return df


def main():
    t0 = datetime.now()

    games_df = insert_game_info(START_DATE, END_DATE, LEAGUES, SEASONS)

    get_all_game_info(games_df, pred=True)

    logger.info("All stats for the selected dates were inserted")

    t1 = datetime.now()

    logger.info("The process took {}".format(t1 - t0))

    return None


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])
    main()
