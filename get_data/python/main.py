import os
import sys
import logging
import logging.config

from constants import END_DATE, PROJECT_DIR, START_DATE, STATS_TABLES
from db_handle import build_standard_tables, create_tables
from get_games import insert_game_info
from get_squads import insert_teams_players
from get_team_stats import insert_teams_stats
from get_game_events import insert_game_events
from get_player_stats import insert_players_stats

sys.path.append(PROJECT_DIR)

from config import config

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def main(first_config=False):
    if first_config:
        build_standard_tables()
        create_tables(STATS_TABLES)
        insert_teams_players()

    games_df = insert_game_info(START_DATE, END_DATE)

    number_of_games = games_df.shape[0]

    n = 1
    for game_id, g in games_df.groupby('game_id'):
        teams = [g['home_team_id'].iloc[0], g['away_team_id'].iloc[0]]
        logger.info(
            "{}/{} - Getting data for game {}".format(
                n, number_of_games, game_id
            )
        )
        insert_teams_stats(game_id)
        insert_players_stats(game_id)
        insert_game_events(game_id, teams)

        n += 1

    logger.info("All stats for the selected dates were inserted")

    return None


if __name__ == '__main__':
    from datetime import datetime

    first_config = True

    logging.config.dictConfig(config['logger'])
    t0 = datetime.now()
    main(first_config)
    t1 = datetime.now()
    logger.info("The process took {}".format(t1 - t0))
