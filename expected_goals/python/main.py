import os
import sys
import logging
import logging.config

from datetime import datetime

from xg_model import get_model
from constants import PROJECT_DIR
from db_handle import get_df_from_query
from get_model_variables import process_game
from get_predictive_stats import insert_predictive_stats

sys.path.append(PROJECT_DIR)

from config import config

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')

os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def main(query):
    t0 = datetime.now()
    df = get_df_from_query(query)
    processed_df = process_game(df)
    predicted_df = get_model(processed_df)
    insert_predictive_stats(predicted_df)
    t1 = datetime.now()
    logger.info("The process took {}".format(t1 - t0))

    return predicted_df


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])

    query = """
    select * from
    (
    select * from game_events
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
    """
    predicted_df = main(query)
