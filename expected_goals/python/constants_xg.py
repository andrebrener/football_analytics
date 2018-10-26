import os

from datetime import date

from sklearn.ensemble import RandomForestRegressor

# Dates
START_DATE = date(2018, 10, 6)
END_DATE = date(2018, 10, 10)

# Relative Paths
PYTHON_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = PYTHON_DIR[:PYTHON_DIR.index('python')]
REPO_DIR = PROJECT_DIR[:PROJECT_DIR.index('expected_goals')]
LIB_COMMON_DIR = os.path.join(REPO_DIR, 'lib_common')
MODEL_FILES_DIR = os.path.join(PROJECT_DIR, 'model')

# Start or pos transition in possession_id
POS_START_IDS = [1, 4]

# Set piece shots standard ids
FK_STANDARD = 4
PK_STANDARD = 6
SET_PIECE_STANDARDS = [FK_STANDARD, PK_STANDARD]

# Action Codes
GOAL_IDS = [8010]
OWN_GOAL_IDS = [8020]

RED_CARD_IDS = [3030]

ATTACKING_PASS_CODES = [1011, 1031]
CROSS_CODES = [26001, 28043]

# Air challenges
# Accurate crossing from set piece with a shot
# Accurate crossing from set piece with a goal

REMOVE_CODES = [2020, 28045, 28044]

# Assists to passes
ASSIST_DICT = {1050: 1031, 1040: 1010, 1000: 1061}

# Id Cols
ID_COLS = ['game_id', 'sequence_number']

# Cols that are manually dummies and their values
FIXED_DUMMIES = {
    'prev_action_id': [],
    'pos_started_zone_id': [str(x) for x in range(1, 26)],
    'position_id': [
        '12', '13', '14', '15', '16', '22', '23', '24', '25', '26', '31', '32',
        '33', '34', '35', '36', '42', '43', '44', '45', '46', '52', '53', '54',
        '55', '56', '100'
    ],
    'len_range': ['very_short', 'short', 'far', 'very_far'],
    'in_first_11': ['0', '1'],
    'is_key': ['0', '1'],
    'home_away': ['home', 'away'],
    'attack_type_id': [str(x) for x in range(1, 7)],
    'play_standard_id': [str(x) for x in range(1, 9)],
    'cons_passes_in_pos_done_range': ['no_pass', 'few', 'many', 'too_many'],
    'game_minutes_range': [
        'first_fifteen', 'second_fifteen', 'third_fifteen', 'injury_time'
    ],
    'possession_time_range': ['very_short', 'short', 'long', 'very_long']
}

for c in ['x', 'y']:
    FIXED_DUMMIES['pos_{}_velocity_range'.format(c)] = [
        'very_slow', 'slow', 'fast', 'very_fast'
    ]

for c in ['half', 'player_foot_id']:
    FIXED_DUMMIES[c] = [1, 2]

for c in ['pos_dest_x_range', 'pos_x_range']:
    FIXED_DUMMIES[c] = ['very_close', 'close', 'mid_range', 'long_range']

for c in ['pos_dest_y_range', 'pos_y_range']:
    FIXED_DUMMIES[c] = ['wide_right', 'right', 'center', 'left', 'wide_left']

for _ in ['', 'prev_']:
    FIXED_DUMMIES['{}body_id'.format(_)] = [str(x) for x in range(1, 6)]

for _ in ['own', 'rival']:
    FIXED_DUMMIES['{}_score_range'.format(_)
                  ] = [str(x) for x in range(9)] + ['more_than_8']
    FIXED_DUMMIES['{}_players'.format(_)] = [str(x) for x in range(7, 12)]
    FIXED_DUMMIES['{}_shot_number_range'.format(_)] = [
        'very_few', 'few', 'many', 'too_many'
    ]

    FIXED_DUMMIES['{}_formation'.format(_)] = [
        '3-1-4-2', '3-5-2', '4-1-4-1', '4-2-3-1', '4-3-3 down',
        '4-4-2 classic', '4-4-2 diamond', '4-3-3 up', '3-4-3', '3-5-2'
    ]
# Target col
TARGET_COL = ['goal']

# Predictive cols to insert
COLS_TO_INSERT = [
    'sequence_number', 'game_id', 'xg', 'xa', 'pos_xg_chain', 'pos_xg_buildup'
]

# Model
MODEL_PARAMS = {
    'n_estimators': [150, 200, 250, 300],
    'max_features': ["auto", 20, 25, 30],
    "bootstrap": [True],
    "min_samples_leaf": [1, 3, 5],
    'max_depth': [5, 10, 15]
}

MODEL = (RandomForestRegressor(), MODEL_PARAMS)
