import os
import sys
import logging
import logging.config

from datetime import datetime

import pandas as pd

from api_calls import get_api_call
from constants import (
    LEAGUES, PLAYERS_IN_TEAM_TPL, PLAYERS_IN_TEAM_URL, PLAYERS_INFO_COLS,
    PLAYERS_INFO_TPL, PLAYERS_INFO_URL, PROJECT_DIR, SEASONS, TEAMS_INFO_TPL,
    TEAMS_INFO_URL
)
from db_handle import get_df_from_query, insert_values

sys.path.append(PROJECT_DIR)

from config import config

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def get_teams_info():

    total_teams_dfs = []

    for l in LEAGUES:
        logger.info("Getting teams for tournament {}".format(l))
        for s in SEASONS:
            json_data = get_api_call(
                TEAMS_INFO_URL.format(TEAMS_INFO_TPL, l, s)
            )

            teams = json_data['data']['row']

            for t in teams:
                team_dict = {k: [v] for k, v in t.items()}

                team_df = pd.DataFrame(team_dict)

                total_teams_dfs.append(team_df)

    teams_df = pd.concat(total_teams_dfs, sort=True)

    teams_df['created_on'] = datetime.now()

    return teams_df


def get_players_info(teams_df):
    total_players_dfs = []
    teams = teams_df['id']
    team_numbers = teams.shape[0]
    n = 1
    for t in teams:
        logger.info(
            "{}/{} - Getting players for team {}".format(n, team_numbers, t)
        )
        json_data = get_api_call(
            PLAYERS_IN_TEAM_URL.format(PLAYERS_IN_TEAM_TPL, t)
        )

        n += 1
        players = json_data['data']['row']

        team_players = [p['id'] for p in players]

        for p in team_players:
            player_json = get_api_call(
                PLAYERS_INFO_URL.format(PLAYERS_INFO_TPL, p)
            )

            player_data = player_json['data']['row']
            for p in player_data:

                player_dict = {k: [v] for k, v in p.items() if k != 'ts'}

                player_df = pd.DataFrame(player_dict)

                total_players_dfs.append(player_df)

    players_df = pd.concat(total_players_dfs, sort=True)
    players_df['created_on'] = datetime.now()

    cols = list(PLAYERS_INFO_COLS.keys())

    return players_df[cols]


def insert_df(df, table, stats_dict):
    query = 'select id from {}'.format(table)
    ids_inserted_df = get_df_from_query(query)
    ids_inserted = []
    if not ids_inserted_df.empty:
        ids_inserted = ids_inserted_df['id'].tolist()
    df['id'] = df['id'].astype(int)
    df_to_insert = df[~df['id'].isin(ids_inserted)]
    if not df_to_insert.empty:
        insert_values(df_to_insert, table, stats_dict)
    else:
        logger.info("All values in {} are already inserted".format(table))


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
