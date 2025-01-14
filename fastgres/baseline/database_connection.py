import configparser
import dataclasses
import time
import re
import psycopg2 as pg

from fastgres.hinting import HintSet, Hint
from tqdm import tqdm
from typing import Optional


@dataclasses.dataclass
class QueryResult:
    query: str
    hint_set_int: int
    time: float
    timeout_used: float
    timed_out: bool
    pre_warmed: bool
    query_plan: dict


def startup_aware(retry_time: float, retries: int = 3):
    """
    This is a custom decorator that is used to mark functions that may require a certain startup time for a DBMS like
    waiting for a docker container to restart.
    :param retry_time: How long to wait until the inner function should be called again.
    :param retries: Number of times to retry the inner function until stopping.
    :return:
    """
    def decorator(f):
        def inner(*args, **kwargs):
            tried = 0
            while True:
                try:
                    return f(*args, **kwargs)
                except pg.OperationalError as e:
                    if "the database system is starting up" in str(e):
                        print(f"Database is still starting up. Retrying in {retry_time}s.")
                        time.sleep(retry_time)
                        continue
                    else:
                        """Non startup messages are retried only a few times"""
                        if tried >= retries:
                            raise e
                        else:
                            print(f"Encountered Database Operational Error, retrying in {retry_time}s.")
                            tried += 1
                            time.sleep(retry_time)
                            continue
        return inner
    return decorator


class DatabaseConnection:

    def __init__(self, psycopg_connection_string: str, name: str = ''):
        self.connection_string = psycopg_connection_string
        self.name = name
        self._connection = None
        self._cursor = None
        self._schema = None
        self._extension_loaded = False
        self._version = None

    def __str__(self):
        return f"Database Connection: {self.name} (Version: {self.version()})"

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.establish_connection()
        return self._connection

    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self.connection.cursor()
        return self._cursor

    @property
    def tables(self):
        if self._schema is None:
            self.cursor.execute("SELECT table_name "
                                "FROM information_schema.tables "
                                "WHERE table_schema = 'public'")
            schema = [_[0] for _ in self.cursor.fetchall()]
            self._schema = list(sorted(schema))
        return self._schema

    def get_columns(self, table: str):
        self.cursor.execute(f"Select * FROM {table} LIMIT 0")
        return [desc[0] for desc in self.cursor.description]

    def get_columns_and_types(self, table: str):
        self.cursor.execute("SELECT column_name, data_type "
                            "FROM information_schema.columns "
                            f"WHERE table_name = '{table}';")
        return self.cursor.fetchall()

    def get_min_max(self, column: str, table: str):
        self.cursor.execute(f"SELECT min({column}), max({column}) FROM {table};")
        return self.cursor.fetchall()[0]

    def get_num_entries(self, table: str):
        self.cursor.execute(f"SELECT reltuples AS estimate FROM pg_class where relname = '{table}';")
        res = self.cursor.fetchall()[0][0]
        # This means no estimate was found and there should probably be an analyze-step
        if res == -1:
            self.cursor.execute(f"Select COUNT(*) FROM {table}")
            res = self.cursor.fetchall()[0][0]
        return res

    def close_connection(self):
        self.close_cursor()
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._extension_loaded = False

    def close_cursor(self):
        if self._cursor is not None:
            self._cursor.close()
        self._cursor = None

    def establish_connection(self):
        try:
            connection = pg.connect(self.connection_string)
            # https://www.psycopg.org/psycopg3/docs/basic/transactions.html#transactions
            connection.autocommit = True
        except ConnectionError:
            raise ConnectionError('Could not connect to database server')
        self._connection = connection
        self.reset_statement_timeout()
        return self._connection

    def version(self, long: bool = False):
        if self._version is None:
            cursor = self.cursor
            self.cursor.execute("SELECT version();")
            postgres_version = cursor.fetchall()[0][0]
            self.close_cursor()
            self._version = postgres_version
        pg_v = self._version
        if not long:
            match = re.findall(r"PostgreSQL\s\d+.\d+", self._version)
            pg_v = match[0][11:]
        return pg_v

    @property
    def schema_info(self):
        if self._schema is None:
            self.cursor.execute("SELECT table_name "
                                "FROM information_schema.tables "
                                "WHERE table_schema = 'public'")
            schema = [_[0] for _ in self.cursor.fetchall()]
            self._schema = list(sorted(schema))
            self.close_cursor()
        return self._schema

    @startup_aware(2.0)
    def set_postgres_config(self, config_path: str) -> bool:
        cfg = configparser.ConfigParser()
        cfg.read(config_path)
        system_settings = dict(cfg.items('SystemSettings'))
        try:
            for system_parameter, value in system_settings.items():
                q = f"ALTER SYSTEM SET {system_parameter} = {value};"
                self.cursor.execute(q)
        except pg.DatabaseError as e:
            self.close_connection()
            raise e
        self.close_cursor()
        return True

    @startup_aware(2.0)
    def verify_postgres_config(self, config_path: str) -> bool:
        cfg = configparser.ConfigParser()
        cfg.read(config_path)
        system_settings = dict(cfg.items("SystemSettings"))
        for system_parameter, value in system_settings.items():
            q = f"SHOW {system_parameter};"
            self.cursor.execute(q)
            set_setting = self.cursor.fetchall()[0][0]
            self.close_cursor()
            if value.strip("';") != set_setting:
                self.close_connection()
                raise ValueError("Set System Setting was not applied! Try restarting the server.")
        return True

    @startup_aware(2.0)
    def reset_postgres_config(self) -> bool:
        try:
            self.cursor.execute("ALTER SYSTEM RESET ALL;")
            self.close_cursor()
        except pg.DatabaseError as e:
            raise e
        return True

    def disable_geqo(self):
        self.cursor.execute("SET geqo = false;")
        self.close_cursor()

    @staticmethod
    def _get_hint_statements(hint_set: HintSet):
        statement = ""
        for i in range(hint_set.collection.collection_size):
            name = hint_set.get_hint(i).database_instruction
            value = hint_set.get(i)
            statement += f"SET {name}={value};\n"
        return statement

    def _build_pre_statement(self, hint_set: HintSet, timeout: Optional[float]):
        statement = ""
        if timeout is not None and timeout > 0.0:
            adjusted_timeout = max(int(timeout), 500)
            statement += f"SET LOCAL statement_timeout = '{adjusted_timeout}ms';\n"
        elif timeout == 0.0:
            statement += "SET LOCAL statement_timeout = '0ms';\n"
        statement += self._get_hint_statements(hint_set)
        return statement

    def evaluate_hinted_query(self, query: str, hint_set: HintSet, timeout: float = None,
                              suppress_timeout_message: bool = True, pre_warm: bool = False,
                              explain_analyze: bool = False) -> QueryResult:

        if timeout is None:
            raise ValueError("Invalid timeout: None")

        if pre_warm:
            self.evaluate_hinted_query(query, hint_set, timeout)
        statement = self._build_pre_statement(hint_set, timeout)
        statement += query
        if explain_analyze:
            statement += "EXPLAIN (ANALYZE, FORMAT JSON, BUFFERS, SETTINGS) " + query
        try:
            start = time.perf_counter_ns()
            self.cursor.execute(statement)
            result_time = (time.perf_counter_ns() - start) / 1_000_000
        except pg.OperationalError as e:
            if 'canceling statement due to statement timeout' in str(e).lower():
                if not suppress_timeout_message:
                    tqdm.write(f"Timeout: {str(e)}")
                result_time = None
                self.connection.cancel()
            else:
                raise
        query_plan = self.cursor.fetchall() if explain_analyze else dict()
        query_result = QueryResult(query=query, hint_set_int=hint_set.hint_set_int,
                                   time=timeout if result_time is None else result_time, timeout_used=timeout,
                                   timed_out=True if result_time is None else False, pre_warmed=pre_warm,
                                   query_plan=query_plan)
        self.close_cursor()
        return query_result

    def explain_query(self, query: str, hint_set: HintSet) -> dict:
        statement = self._build_pre_statement(hint_set, 0)
        statement += "EXPLAIN (FORMAT JSON) " + query
        self.cursor.execute(statement)
        query_plan = self.cursor.fetchall()[0][0][0]["Plan"]
        self.close_cursor()
        return query_plan

    @staticmethod
    def _get_hint_status_statement(hint: Hint):
        return f"show {hint.name};"

    def get_hint_status(self, hint: Hint):
        self.cursor.execute(self._get_hint_status_statement(hint))
        res = self.cursor.fetchall()
        self.close_cursor()
        return res

    def reset_statement_timeout(self):
        self.cursor.execute("SET statement_timeout to default;")
        self.close_cursor()

    def get_statement_timeout(self):
        self.cursor.execute("SHOW statement_timeout;")
        res = self.cursor.fetchall()[0][0]
        self.close_cursor()
        return res
