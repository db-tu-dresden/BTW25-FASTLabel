
import os
from configparser import ConfigParser

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


class PathConfig:
    """
    This is a utility class to supply database connection information through a configuration file. The path should
    point to a directory that contains a config.ini file
    """
    def __init__(self, path: str):
        self.path = path
        self.ROOT_DIR = os.path.dirname(self.path)
        self.LOG_DIR = os.path.join(self.ROOT_DIR, "logs")
        cfg = ConfigParser()
        self.config_file_path = os.path.join(self.ROOT_DIR, "config.ini")
        cfg.read(self.config_file_path)

        try:
            self.dbs = cfg["DBConnections"]
        except KeyError:
            raise KeyError(f"Key error at config path: {self.config_file_path}")

        self._PG_IMDB = None
        self._PG_STACK_OVERFLOW = None
        self._PG_TPC_H = None

    @property
    def PG_IMDB(self):
        """
        Standard Parameter for the IMDB database that has to be present in every config file
        :return: IMDB connection parameters in psycopg2 format.
        """
        if self._PG_IMDB is None:
            self._PG_IMDB = self.dbs["imdb"]
        return self._PG_IMDB

    @property
    def PG_STACK_OVERFLOW(self):
        """
            Standard Parameter for the Stack database that has to be present in every config file
            :return: Stack connection parameters in psycopg2 format.
        """
        if self._PG_STACK_OVERFLOW is None:
            self._PG_STACK_OVERFLOW = self.dbs["stack_overflow"]
        return self._PG_STACK_OVERFLOW

    def get_db_connection(self, connection_key: str):
        """
        Custom function for other psycopg2 conform database entries
        :param connection_key: key-parameter to look for in the config file
        :return: Connection parameters in psycopg2 format.
        """
        return self.dbs[connection_key]
