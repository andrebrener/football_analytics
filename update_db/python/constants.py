import os

from datetime import date

# Variables

START_DATE = date(2018, 10, 20)
END_DATE = date(2018, 10, 21)

# Leagues
LEAGUES = [93]
SEASONS = [22]

# Squad update leagues
SQUAD_UPDATE_DICT = {93: 22}

# Relative Paths
PYTHON_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = PYTHON_DIR[:PYTHON_DIR.index('python')]
REPO_DIR = PROJECT_DIR[:PROJECT_DIR.index('update_db')]
LIB_COMMON_DIR = os.path.join(REPO_DIR, 'lib_common')
GET_DATA_DIR = os.path.join(REPO_DIR, 'get_data', 'python')
XG_DIR = os.path.join(REPO_DIR, 'expected_goals', 'python')
