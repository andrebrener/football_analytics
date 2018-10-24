import os

from db_data import DB_HOST, DB_NAME, DB_USERNAME
from dbconnectors import PostgreSqlDb

db = PostgreSqlDb(username=DB_USERNAME, host=DB_HOST, database=DB_NAME)


def create_tables(project_dir, tables):
    sql_path = os.path.join(project_dir, 'sql', '{}.sql')
    for t in tables:
        query = open(sql_path.format(t), 'r').read()
        db.execute('DROP TABLE IF EXISTS {} CASCADE'.format(t))
        db.execute(query)


def insert_values(df, table_name, stats_dict):
    df.rename(columns=stats_dict, inplace=True)

    db.insert_from_frame(df, table_name)

    return None


def get_df_from_query(query):
    df = db.get_pandas_df(query)
    return df


if __name__ == '__main__':

    query = 'select id from teams_info'

    df = get_df_from_query(query)

    import pdb; pdb.set_trace()  # noqa # yapf: disable
