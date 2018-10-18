import os
import sys
import logging
import logging.config

import pandas as pd

from api_calls import get_api_call
from constants import EVENTS_TPL, GAME_EVENTS_COLS, GAME_INFO_URL, PROJECT_DIR
from db_handle import insert_values
from process_game import process_game

sys.path.append(PROJECT_DIR)

from config import config

logger = logging.getLogger('main_logger')

LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)


def get_game_events(game_id, teams):

    json_data = get_api_call(GAME_INFO_URL.format(EVENTS_TPL, game_id))

    events = json_data['data']['row']

    events_dfs = []
    for ev in events:
        ev_dict = {}
        for key, val in ev.items():
            ev_dict[key] = [val]

        events_dfs.append(pd.DataFrame(ev_dict))

    game_df = pd.concat(events_dfs, sort=True)

    game_df['game_id'] = game_id

    game_df['home_away'] = game_df.apply(
        lambda x: get_home_away(x, teams), axis=1
    )

    return game_df


def get_home_away(df, teams):
    home_team, away_team = teams

    if df['team_id'] == home_team:
        return 'home'
    elif df['team_id'] == away_team:
        return 'away'
    else:
        return None


def insert_game_events(game_id, teams):
    game_df = process_game(get_game_events(game_id, teams))
    insert_values(game_df, 'game_events', GAME_EVENTS_COLS)
    logger.info("Inserted events for game {}".format(game_id))
    return None


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])

    # game_data = ('1234775', ['857', '846'])
    # game_data = ('1234769', ['11052', '861'])
    game_data = ('1234767', ['853', '844'])

    # insert_game_events(game_data[0], game_data[1])
    df = process_game(get_game_events(game_data[0], game_data[1]))

    import pdb; pdb.set_trace()  # noqa # yapf: disable
