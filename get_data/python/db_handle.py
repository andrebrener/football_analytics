import os
import sys
import logging
import logging.config

import pandas as pd

from constants import LIB_COMMON_DIR, PROJECT_DIR, STANDARD_TABLES

sys.path.append(LIB_COMMON_DIR)
sys.path.append(PROJECT_DIR)

from config import config
from db_data import DB_HOST, DB_NAME, DB_USERNAME
from dbconnectors import PostgreSqlDb

logger = logging.getLogger('main_logger')
LOGS_DIR = os.path.join(PROJECT_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.chdir(LOGS_DIR)

db = PostgreSqlDb(username=DB_USERNAME, host=DB_HOST, database=DB_NAME)


def create_tables(tables):
    sql_path = os.path.join(PROJECT_DIR, 'sql', '{}.sql')
    for t in tables:
        query = open(sql_path.format(t), 'r').read()
        db.execute('DROP TABLE IF EXISTS {} CASCADE'.format(t))
        db.execute(query)
        logger.info("{} table created".format(t))


def insert_values(df, table_name, stats_dict):
    df.rename(columns=stats_dict, inplace=True)

    db.insert_from_frame(df, table_name)

    return None


def build_standard_tables():
    create_tables(STANDARD_TABLES)
    for t in STANDARD_TABLES:
        file_path = os.path.join(PROJECT_DIR, 'csvs', '{}.csv'.format(t))
        df = pd.read_csv(file_path)

        stats_dict = {c: c for c in df.columns}

        insert_values(df, t, stats_dict)


def get_df_from_query(query):
    df = db.get_pandas_df(query)
    if df.empty:
        logger.info("Query returned empty df")

    return df


if __name__ == '__main__':
    logging.config.dictConfig(config['logger'])

    # build_standard_tables()

    query = 'select id from teams_info'
    df = get_df_from_query(query)

    import pdb; pdb.set_trace()  # noqa # yapf: disable
