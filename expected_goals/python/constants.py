import os

from datetime import date

# Variables

START_DATE = date(2018, 10, 7)
END_DATE = date(2018, 10, 7)

# END_DATE = date.today() - timedelta(1)
# START_DATE = END_DATE - timedelta(3)

# Leagues
LEAGUES = [93]
SEASONS = [22]

# Relative Paths
PYTHON_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = PYTHON_DIR[:PYTHON_DIR.index('python')]
REPO_DIR = PROJECT_DIR[:PROJECT_DIR.index('expected_goals')]
LIB_COMMON_DIR = os.path.join(REPO_DIR, 'lib_common')

# Variables for the model

VAR_COLS = [
    'half', 'game_minutes', 'game_seconds', 'home_away', 'body_id',
    'direction', 'position_id', 'cons_passes_in_pos_done', 'attack_type_id',
    'in_first_11', 'player_foot_id', 'is_shot', 'possession_time'
]

for _ in ['own', 'rival']:
    for c in ['score', 'players', 'shot_number', 'formation']:
        VAR_COLS.append('{}_{}'.format(_, c))

for coord in ['x', 'y']:
    for c in ['pos', 'pos_dest']:
        VAR_COLS.append('{}_{}'.format(c, coord))
        if c == 'pos':
            VAR_COLS.append('pos_started_{}_{}'.format(c, coord))
            VAR_COLS.append('{}_{}_velocity'.format(c, coord))

for c in ['pos_x', 'pos_y', 'action_id', 'body_id', 'len']:
    VAR_COLS.append('prev_{}'.format(c))

# start or pos transition in possession_id
POS_START_IDS = [1, 4]

# Action Codes
GOAL_IDS = [8010]
OWN_GOAL_IDS = [8020]

RED_CARD_IDS = [3030]

# Air challenges
# Crosses accurate
# Accurate crossing from set piece with a shot
REMOVE_CODES = [2020, 26001, 28044]

# Assists to passes

ASSIST_DICT = {1050: 1031, 1040: 1010, 1000: 1061}
