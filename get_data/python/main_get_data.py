import os
import sys
import logging
import logging.config

import pandas as pd

from get_games import insert_game_info
from get_squads import insert_teams_players
from constants_gd import (
    END_DATE, LEAGUES, LIB_COMMON_DIR, PROJECT_DIR, SEASONS, STANDARD_TABLES,
    START_DATE, STATS_TABLES, UD_DIR, XG_DIR
)
from get_team_stats import insert_teams_stats
from get_game_events import insert_game_events
from get_player_stats import insert_players_stats

for d in [XG_DIR, PROJECT_DIR, LIB_COMMON_DIR]:
    sys.path.append(d)

from config import config
from main_xg import main_xg
from db_handle import create_tables, get_df_from_query, insert_values

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def build_standard_tables():
    create_tables(PROJECT_DIR, STANDARD_TABLES)
    for t in STANDARD_TABLES:
        file_path = os.path.join(PROJECT_DIR, 'csvs', '{}.csv'.format(t))
        df = pd.read_csv(file_path)

        stats_dict = {c: c for c in df.columns}

        insert_values(df, t, stats_dict)
    logger.info("Standard tables created & inserted")


def get_processed_game(game_id):
    sql_path = os.path.join(UD_DIR, 'sql', 'processed_game.sql')
    query = open(sql_path, 'r').read().format(game_id=game_id)
    df = get_df_from_query(query)
    return df


def get_all_game_info(games_df, pred=True):
    n = 1

    number_of_games = games_df.shape[0]
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

        if pred:
            logger.info("Getting predictive stats")

            df = get_processed_game(game_id)

            main_xg(df)

        n += 1

    return None


def main_get_data(setup=False):
    if setup:
        start_from_zero = input(
            "This will restart the whole db. Are you sure you want to run it? Insert y or n: "
        )
        if start_from_zero == 'y':
            build_standard_tables()
            create_tables(PROJECT_DIR, STATS_TABLES)
            logger.info("Stats tables created & inserted")
            insert_teams_players()
        else:
            sys.exit()

    games_df = insert_game_info(START_DATE, END_DATE, LEAGUES, SEASONS)

    get_all_game_info(games_df, pred=False)

    logger.info("All stats for the selected dates were inserted")

    return None


if __name__ == '__main__':
    from datetime import datetime

    setup = True

    logging.config.dictConfig(config['logger'])
    t0 = datetime.now()
    main_get_data(setup)
    t1 = datetime.now()
    logger.info("The process took {}".format(t1 - t0))
