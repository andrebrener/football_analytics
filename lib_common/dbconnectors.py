import os
import six
import sys
import logging
import operator

from abc import ABCMeta, abstractmethod
from time import sleep
from random import randint
from functools import reduce
from collections import namedtuple

import yaml
import progressbar
import pandas as pd
import pyhive.exc
import sqlalchemy.exc
import pandas.io.sql as psql

from pyhive import presto
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class BaseDb:

    _DIALECT = None
    _CONNECTION_FIELDS = (
        'driver', 'username', 'password', 'host', 'catalog', 'database', 'port'
    )
    _EXTRA_CONNECTION_FIELDS = ()
    _REQUIRED_FIELDS = ()

    def __init__(self, max_retries=5, max_backoff=64, **kwargs):
        self.dialect = self._DIALECT
        self._connection_fields = (
            self._CONNECTION_FIELDS + self._EXTRA_CONNECTION_FIELDS
        )
        self._engine = None
        self._max_retries = max_retries
        self._max_backoff = max_backoff

        for field in self._connection_fields:
            setattr(self, field, kwargs.get(field, None))
        for field in self._REQUIRED_FIELDS:
            if getattr(self, field) is None:
                raise TypeError("'{}' not specified".format(field))

    @staticmethod
    def _load_config(config_file):
        with open(config_file, 'r') as config_file:
            return yaml.safe_load(config_file.read())

    @classmethod
    def from_config(cls, config_file='credentials.yaml', keys=[]):
        conf = reduce(operator.getitem, keys, cls._load_config(config_file))
        return cls(**conf)

    def get_connection(self):
        d = {
            x: getattr(self, x)
            for x in dir(self)
            if x in (self._connection_fields + ('dialect', ))
        }
        return namedtuple('Connection', d.keys())(**d)

    def _get_db_uri(self):
        conn = self.get_connection()
        dialect = conn.dialect + (
            '+{}'.format(conn.driver) if conn.driver else ''
        )
        login = conn.username + (
            ':{}'.format(conn.password) if conn.password else ''
        )
        host = conn.host + (':{}'.format(conn.port) if conn.port else '')
        database = ('{}/'.format(conn.catalog)
                    if conn.catalog else '') + conn.database
        return '{dialect}://{login}@{host}/{db}'.format(
            dialect=dialect, login=login, host=host, db=database
        )

    def _get_engine(self, custom_uri=None, connect_args={}):
        if not self._engine:
            db_uri = custom_uri or self._get_db_uri()
            self._engine = create_engine(db_uri, connect_args=connect_args)
        return self._engine

    @abstractmethod
    def _connect(self):
        pass

    def _cursor(self):
        conn = self._connect()
        try:
            return conn.cursor()
        except AttributeError:
            # If the connection object is an sqlalchemy Connection then we
            # need to access the cursor through the connection attribute
            return conn.connection.cursor()

    @staticmethod
    def query_statement_preprocessing(func):

        def wrapper(self, *args, **kwargs):
            args = list(args)
            # SQL query must be the first named argument (it can either be
            # a filepath or a string)
            sql = args[0]
            if os.path.isfile(sql):
                with open(sql, 'r') as f:
                    sql = f.read()

            format_params = kwargs.get('params', None)
            if format_params:
                try:
                    sql = sql.format(**format_params)
                except KeyError:
                    # Don't raise error if the format key is not present in our
                    # formatting dict
                    pass

            logger.debug('Executing the following query: \n' + sql)
            args[0] = sql
            return func(self, *args, **kwargs)

        return wrapper

    @query_statement_preprocessing.__func__
    def execute(self, sql, params={}):
        with self._connect() as connection:
            return connection.execute(sql)

    def insert_from_frame(
        self, df, table, if_exists='append', index=False, **kwargs
    ):
        # TODO: Consider using odo to insert large dataframes (i.e add
        # bulk_load method)
        # See: https://github.com/pandas-dev/pandas/issues/8953
        connection = self._connect()
        if self._engine is None:  # Only for sqalchemy
            raise NotImplementedError(
                "Inserting dataframes using `{}` is currently not "
                "implemented.".format(self.dialect)
            )
        with connection:
            df.to_sql(
                table, connection, if_exists=if_exists, index=index, **kwargs
            )

    def upsert_from_frame(self, df, table, how={}, **kwargs):
        df = df.where(pd.notnull(df), None)  # convert nulls to none type
        cols = ','.join(df.columns)
        vals = ','.join(map(str, df.to_records(index=False).tolist()))
        sql = "INSERT INTO {table} ({columns}) VALUES {values}".format(
            table=table, columns=cols, values=vals
        )
        self._execute_upsert(sql, table, how, **kwargs)

    def _execute_upsert(self):
        raise NotImplementedError(
            "Upserting dataframes using `{}` is currently not "
            "implemented.".format(self.dialect)
        )

    @query_statement_preprocessing.__func__
    def to_frame(self, sql, params={}, **kwargs):
        with self._connect() as connection:  # connect only once
            for i in range(1, self._max_retries + 1):
                try:
                    return psql.read_sql(sql, connection, **kwargs)
                except (
                    sqlalchemy.exc.ProgrammingError,
                    sqlalchemy.exc.OperationalError
                ) as e:
                    # Don't retry if there are programming errors (such as
                    # syntax errors)
                    logger.error(
                        "Programming/Operational error: {}".
                        format(e.orig.args)
                    )
                    raise
                except sqlalchemy.exc.DatabaseError as e:
                    if i == self._max_retries:
                        logger.error(
                            "Reached max number ({}) of query retries. "
                            "Aborting!".format(self._max_retries)
                        )
                        raise
                    else:
                        backoff_time = min(
                            self._max_backoff,
                            2**i + (randint(0, 1000) / 1000)
                        )
                        logger.error("Database error: {}".format(e.orig.args))
                        logger.info(
                            "Query has failed {} times. Retrying in {:.2f} "
                            "seconds...".format(i, backoff_time)
                        )
                        sleep(backoff_time)

    @query_statement_preprocessing.__func__
    def get_pandas_df(self, sql, params={}, **kwargs):
        return self.to_frame(sql, params={}, **kwargs)


class MySqlDb(BaseDb):

    _DIALECT = 'mysql'
    _REQUIRED_FIELDS = ('username', 'host', 'database')

    def __init__(self, driver='mysqlconnector', **kwargs):
        super(MySqlDb, self).__init__(driver=driver, port=3306, **kwargs)

    def _connect(self):
        return self._get_engine().connect()

    def _execute_upsert(self, sql, table, how, **kwargs):
        sql += '\nON DUPLICATE KEY UPDATE\n'
        sql += ',\n'.join(
            [
                '{k} = {u}'.format(
                    k=k,
                    u='{k} + VALUES({k})'.format(k=k)
                    if v == 'agg' else 'VALUES({k})'.format(k=k)
                    if v == 'replace' else '{k} + {v}'.format(k=k, v=v)
                ) for k, v in six.iteritems(how)
            ]
        )
        # Note: with mysql we seem to need `NULL` for nan values
        sql = sql.replace('None', 'NULL')
        return self.execute(sql)


class PostgreSqlDb(BaseDb):

    _DIALECT = 'postgresql'
    _REQUIRED_FIELDS = ('username', 'host', 'database')

    def __init__(self, driver='psycopg2', **kwargs):
        super(PostgreSqlDb, self).__init__(driver=driver, **kwargs)

    def _connect(self):
        return self._get_engine().connect()

    def _execute_upsert(self, sql, table, how, **kwargs):
        # TODO: Consider using cols instead of unique constraint
        unique_constraint = kwargs.get('unique_constraint', None)
        assert unique_constraint is not None, (
            "'unique_constraint' kwarg not specified."
        )
        sql += '\nON CONFLICT ON CONSTRAINT {}\nDO UPDATE SET\n'.format(
            unique_constraint
        )
        sql += ',\n'.join(
            [
                '{k} = {u}'.format(
                    k=k,
                    u='{table}.{k} + EXCLUDED.{k}'.format(table=table, k=k)
                    if v == 'agg' else 'EXCLUDED.{k}'.format(k=k)
                    if v == 'replace' else
                    '{table}.{k} + {v}'.format(table=table, k=k, v=v)
                ) for k, v in six.iteritems(how)
            ]
        )
        return self.execute(sql)


class SqLiteDb(BaseDb):

    _DIALECT = 'sqlite'
    _REQUIRED_FIELDS = ('database', )

    def __init__(self, **kwargs):
        super(SqLiteDb, self).__init__(**kwargs)

    def _connect(self):
        conn = self.get_connection()
        db_uri = '{dialect}:///{db}'.format(
            dialect=conn.dialect, db=conn.database
        )
        return self._get_engine(custom_uri=db_uri).connect()


class PrestoDb(BaseDb):

    _DIALECT = 'presto'
    _EXTRA_CONNECTION_FIELDS = ('session_props', )
    _REQUIRED_FIELDS = ('username', 'host', 'port')

    def __init__(
        self,
        catalog='default',
        database='default',
        session_props={'hash_partition_count': 32},
        **kwargs
    ):
        super(PrestoDb, self).__init__(
            catalog=catalog,
            database=database,
            session_props=session_props,
            **kwargs
        )

    def _connect(self):
        conn = self.get_connection()
        # Define Sqlalchemy engine (we only use it for potential insertion)
        self._get_engine(connect_args={'session_props':
                                       conn.session_props}).raw_connection()
        # Note: Presto does not have a notion of a persistent connection (i.e
        # cursors do all the work)
        return presto.connect(
            host=conn.host,
            port=conn.port,
            username=conn.username,
            catalog=conn.catalog,
            schema=conn.database,
            session_props=conn.session_props
        )

    def _show_query_progress(self, cursor):
        logger.info("Query progress...")
        status = cursor.poll()
        bar = progressbar.ProgressBar()
        while status['stats']['state'] != 'FINISHED':
            if status['stats'].get('totalSplits', 0) > 0:
                pct_complete = round(
                    status['stats']['completedSplits'] /
                    float(status['stats']['totalSplits']), 4
                )
                bar.update(pct_complete * 100)
            status = cursor.poll()
        bar.finish()

    @BaseDb.query_statement_preprocessing
    def execute(self, sql, params={}, show_progress=False):
        cursor = self._cursor()
        for i in range(1, self._max_retries + 1):
            try:
                cursor.execute(sql)
                if show_progress:
                    self._show_query_progress(cursor)
                return cursor
            except (pyhive.exc.OperationalError) as e:
                # Don't retry if there are operational errors (such as
                # invalid connection setttings)
                raise
            except pyhive.exc.DatabaseError as e:
                if len(e.args) and isinstance(e.args[0], dict):
                    err_dict = e.args[0]
                    err_type = err_dict.get('errorName')
                    err_msg = err_dict.get('message')
                    logger.error("{}: {}".format(err_type, err_msg))
                    if err_type == 'SYNTAX_ERROR':
                        raise
                else:
                    logger.error(str(e))
                if i == self._max_retries:
                    logger.error(
                        "Reached max number ({}) of query retries. "
                        "Aborting!".format(self._max_retries)
                    )
                    raise
                else:
                    backoff_time = min(
                        self._max_backoff, 2**i + (randint(0, 1000) / 1000)
                    )
                    logger.info(
                        "Query has failed {} times. Retrying in {:.2f} "
                        "seconds...".format(i, backoff_time)
                    )
                    sleep(backoff_time)
            except KeyboardInterrupt:
                cursor.cancel()
                sys.exit(0)

    def to_frame(self, sql, params={}, show_progress=False):
        cursor = self.execute(sql, params=params, show_progress=show_progress)
        data = cursor.fetchall()
        if data:
            df = pd.DataFrame(data)
            df.columns = [c[0] for c in cursor.description]
        else:
            df = pd.DataFrame()
        return df


class HiveDb(BaseDb):

    _DIALECT = 'hive'
    _REQUIRED_FIELDS = ('host', 'port')

    def __init__(self, driver='impala', database='default', **kwargs):
        super(HiveDb, self).__init__(
            driver=driver, database=database, **kwargs
        )

    def _connect(self):
        conn = self.get_connection()
        db_uri = 'impala://{host}:{port}/{db}'.format(
            host=conn.host, port=conn.port, db=conn.database
        )
        return self._get_engine(custom_uri=db_uri).connect()
