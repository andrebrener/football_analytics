import os
import sys
import logging
import logging.config

from xg_model import get_model
from constants_xg import END_DATE, LIB_COMMON_DIR, PROJECT_DIR, START_DATE
from get_model_variables import process_game
from get_predictive_stats import insert_predictive_stats

sys.path.append(PROJECT_DIR)
sys.path.append(LIB_COMMON_DIR)

from config import config
from db_handle import create_tables, get_df_from_query

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')

os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def main_xg(df, setup=False):
    if setup:
        start_from_zero = input(
            "This will restart the pred_stats table. Are you sure you want to run it? Insert y or n: "
        )
        if start_from_zero == 'y':
            create_tables(PROJECT_DIR, ['pred_stats'])
        else:
            sys.exit()

    ordered_df = df.sort_values(['game_id', 'sequence_number'])
    processed_df = process_game(ordered_df)
    predicted_df = get_model(processed_df)
    insert_predictive_stats(predicted_df)

    return None


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])

    from datetime import datetime

    sql_path = os.path.join(PROJECT_DIR, 'sql', 'get_game_events.sql')
    query = open(sql_path, 'r').read().format(sd=START_DATE, ed=END_DATE)
    df = get_df_from_query(query)

    t0 = datetime.now()
    setup = True
    main_xg(df, setup)
    t1 = datetime.now()
    logger.info("The process took {}".format(t1 - t0))
