
import os
from fastgres.baseline import utility as u
from fastgres.baseline.database_connection import DatabaseConnection
from fastgres.baseline.log_utils import get_logger
from fastgres.query_encoding.encoding_handlers.min_max_dictionary import MinMaxDictionary
from fastgres.query_encoding.encoding_handlers.label_encoders import (build_label_encoders, save_label_encoders_to_json,
                                                                      load_label_encoders_from_json)
from fastgres.query_encoding.encoding_handlers.wildcard_dictionary import build_wildcard_dictionary
from fastgres.workload.workload import Workload


class EncodingInformation:

    def __init__(self, db_connection: DatabaseConnection, path: str, workload: Workload, eager_load: bool = True):
        self.db_connection = db_connection
        self.path = path
        self.min_max_dict = None
        self.label_encoders = None
        self.wildcard_dict = None
        self.db_type_dict = None
        self.skipped_columns = None
        self.workload = workload
        if eager_load:
            self.load_encoding_info()

    def __str__(self):
        return_string = "Using Encoding Information:\n" \
                        + f"Path: {self.path}"
        return return_string

    def load_encoding_info(self):
        try:
            mm_d = u.load_json(self.path + "mm_dict.json")
            self.min_max_dict = MinMaxDictionary.from_dict(mm_d).min_max_dictionary
            self.label_encoders = load_label_encoders_from_json(self.path + "label_encoders.json")
            self.wildcard_dict = u.load_json(self.path + "wildcard_dict.json")
        except ValueError:
            raise ValueError("Exception loading dictionaries")

        db_type_path = self.path + "db_type_dict.json"
        if not os.path.exists(db_type_path):
            self.db_type_dict = self.build_db_type_dict(self.db_connection)
            u.save_json(self.db_type_dict, self.path + "db_type_dict.json")
        else:
            self.db_type_dict = u.load_json(db_type_path)

        # These are expected to be selected manually for now
        skipped_path = self.path + "skipped_table_columns.json"
        if not os.path.exists(skipped_path):
            logger = get_logger()
            logger.info("No skipped table columns found...")
            self.skipped_columns = dict()
        else:
            self.skipped_columns = u.load_json(skipped_path)

    def build_encoding_info(self, rebuild: bool = False):
        logger = get_logger()

        db_type_path = self.path + "db_type_dict.json"
        if not os.path.exists(db_type_path) or rebuild:
            self.db_type_dict = self.build_db_type_dict(self.db_connection)
            u.save_json(self.db_type_dict, self.path + "db_type_dict.json")
        else:
            self.db_type_dict = u.load_json(db_type_path)

        # min max
        if not os.path.exists(self.path + "mm_dict.json") or rebuild:
            mm_dict = MinMaxDictionary()
            mm_dict.build_min_max_dict(self.db_connection)
            u.save_json(mm_dict.to_dict(), self.path + "mm_dict.json")
        else:
            logger.info("MinMax dictionary already exists. Consider using the rebuild option.")

        logger.info(f"Finished building min-max dictionary for database: {self.db_connection.name}")

        # label encoders
        if not os.path.exists(self.path + "label_encoders.json") or rebuild:
            label_encoders = build_label_encoders(self.db_connection)
            save_label_encoders_to_json(label_encoders, self.path + "label_encoders.json")
        else:
            logger.info("Label encoders already exists. Consider using the rebuild option.")

        logger.info(f"Finished building label encoders for database: {self.db_connection.name}")

        # wildcard
        if not os.path.exists(self.path + "wildcard_dict.json") or rebuild:
            if self.db_type_dict is None:
                self.db_type_dict = self.build_db_type_dict(self.db_connection)
            wildcard_dict = build_wildcard_dictionary(self.db_type_dict, self.workload, self.db_connection)
            u.save_json(wildcard_dict, self.path + "wildcard_dict.json")
        else:
            logger.info("Wildcard dictionary already exists. Consider using the rebuild option.")

        logger.info(f"Finished building wildcard dictionary for database: {self.db_connection.name}")

    @staticmethod
    def build_db_type_dict(db_connection: DatabaseConnection):
        d_type_dict = dict()
        for table in db_connection.tables:
            d_type_dict[table] = dict()
            columns_and_types = db_connection.get_columns_and_types(table)
            for column, d_type in columns_and_types:
                d_type_dict[table][column] = d_type
        db_connection.close_connection()
        return d_type_dict
