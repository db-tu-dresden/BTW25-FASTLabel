
from fastgres.baseline.database_connection import DatabaseConnection
from fastgres.baseline.log_utils import get_logger
from tqdm import tqdm

import orjson
from collections import defaultdict
from typing import List, Dict, Any


class FastgresLabelEncoder:
    def __init__(self):
        self.classes_ = []
        self.encoder = {}

    def fit(self, y: List[str], sorty_by: List[int]) -> None:
        if len(y) != len(sorty_by):
            raise ValueError("Length of y and sorty_by must be the same.")

        counts = defaultdict(int)
        for key, count in zip(y, sorty_by):
            counts[str(key)] += count
        sorted_keys = sorted(counts.keys(), key=lambda x: counts[x])

        self.classes_ = sorted_keys
        self.encoder = {key: idx for idx, key in enumerate(sorted_keys)}

    def transform(self, values: List[str]) -> List[int]:
        return [self.encoder.get(item, -1) for item in values]  # Returns -1 if item not found

    def to_dict(self) -> Dict[str, Any]:

        return {
            "classes_": self.classes_,
            "encoder": self.encoder
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FastgresLabelEncoder':
        instance = cls()
        instance.classes_ = data["classes_"]
        instance.encoder = data["encoder"]
        return instance


# Assuming DatabaseConnection and get_logger are defined elsewhere

def build_label_encoders(db_connection: DatabaseConnection) -> Dict[str, Dict[str, Dict[str, Any]]]:
    unhandled = set()
    label_encoders = dict()
    logger = get_logger()

    for table in db_connection.tables:
        for column, d_type in tqdm(db_connection.get_columns_and_types(table), desc=f"Processing {table}"):
            if d_type in {'character varying', 'character'}:
                skip = False
                if "stack_overflow" in db_connection.connection_string:
                    skipped_string_columns = {
                        "account": ["display_name"],
                        "answer": ["title", "body"],
                        "question": ["title", "tagstring", "body"],
                        "site": ["site_name"],
                        "tag": ["name"],
                        "badge": ["name"],
                        "comment": ["body"]
                    }
                    # Check if the current table and column should be skipped
                    if table in skipped_string_columns and column in skipped_string_columns[table]:
                        skip = True
                if skip:
                    continue

                # Execute SQL to get unique values and their counts
                try:
                    query = f"SELECT {column}, COUNT({column}) FROM {table} GROUP BY {column}"
                    db_connection.cursor.execute(query)
                except Exception as e:
                    logger.error(f"Failed to execute query for table: {table}, column: {column}. Error: {e}")
                    continue

                # Fetch all results efficiently
                filter_list = db_connection.cursor.fetchall()  # Assuming it returns a list of tuples

                if not filter_list:
                    logger.warning(f"No data found for table: {table}, column: {column}. Skipping encoder.")
                    continue

                logger.info(f"Fitting label encoder to table: {table}, column: {column}")
                y, sorty_by = zip(*filter_list)
                label_encoder = FastgresLabelEncoder()
                label_encoder.fit(list(y), list(sorty_by))

                # Serialize the encoder
                encoder_dict = label_encoder.to_dict()

                # Organize into the nested dictionary
                if table not in label_encoders:
                    label_encoders[table] = {}
                label_encoders[table][column] = encoder_dict
            else:
                unhandled.add(d_type)

    # Close the database connection
    db_connection.close_connection()

    logger.info(f"Unhandled types for label encoding: {unhandled}")

    return label_encoders


def save_label_encoders_to_json(label_encoders: Dict[str, Dict[str, Dict[str, Any]]], filepath: str) -> None:
    try:
        serialized_data = orjson.dumps(label_encoders)

        with open(filepath, 'wb') as f:
            f.write(serialized_data)
        tqdm.write(f"Successfully saved label encoders to {filepath}")
    except Exception as e:
        tqdm.write(f"Failed to save label encoders to {filepath}. Error: {e}")
        raise


def load_label_encoders_from_json(filepath: str) -> Dict[str, Dict[str, FastgresLabelEncoder]]:
    try:
        with open(filepath, 'rb') as f:
            serialized_data = f.read()

        label_encoders_serialized = orjson.loads(serialized_data)

        # Reconstruct the nested dictionary with FastgresLabelEncoder instances
        label_encoders = {
            table: {
                column: FastgresLabelEncoder.from_dict(encoder_dict)
                for column, encoder_dict in columns.items()
            }
            for table, columns in label_encoders_serialized.items()
        }

        get_logger().info(f"Successfully loaded label encoders from {filepath}")
        return label_encoders
    except Exception as e:
        get_logger().error(f"Failed to load label encoders from {filepath}. Error: {e}")
        raise

# class FastgresLabelEncoder:
#     def __init__(self):
#         self.classes_ = None
#         self.encoder = dict()
#
#     def fit(self, y: list, sorty_by: list) -> None:
#         self.classes_ = pd.Series(y).unique()
#         self.encoder = get_sorted_dict(y, sorty_by)
#         return
#
#     def transform(self, values: list) -> list:
#         return_list = list()
#         for item in values:
#             return_list.append(self.encoder[item])
#         return return_list
#
#
# def get_sorted_dict(values, sort_by):
#     mixed = list(zip(values, sort_by))
#     mixed.sort(key=lambda x: x[1])
#     mixed_dict = {mixed[i][0]: i for i in range(len(values))}
#     return mixed_dict


# def build_label_encoders(db_connection: DatabaseConnection):
#     unhandled = set()
#     label_encoders = dict()
#     logger = get_logger()
#     for table in db_connection.tables:
#         for column, d_type in tqdm(db_connection.get_columns_and_types(table)):
#             if d_type == 'character varying' or d_type == 'character':
#                 skip = False
#                 if "stack_overflow" in db_connection.connection_string:
#                     skipped_string_columns = {
#                         "account": ["display_name"],
#                         "answer": ["title", "body"],
#                         "question": ["title", "tagstring", "body"],
#                         "site": ["site_name"],
#                         "tag": ["name"],
#                         "badge": ["name"],
#                         "comment": ["body"]
#                     }
#                     # skipping all unneeded columns
#                     for skipped_table in skipped_string_columns:
#                         if table == skipped_table and column in skipped_string_columns[skipped_table]:
#                             skip = True
#                             break
#                 if skip:
#                     continue
#
#                 db_connection.cursor.execute(f"SELECT {column}, COUNT({column}) FROM {table} GROUP BY {column}")
#                 filter_list = list()
#                 for filter_value, cardinality in db_connection.cursor.fetchall():
#                     filter_list.append((filter_value, cardinality))
#                 logger.info("Fitting label encoder to table: {}, column: {}".format(t, column))
#                 label_encoder = FastgresLabelEncoder()
#                 label_encoder.fit(*list(zip(*filter_list)))
#                 try:
#                     label_encoders[table][column] = label_encoder
#                 except KeyError:
#                     label_encoders[table] = dict()
#                     label_encoders[table][column] = label_encoder
#             else:
#                 unhandled.add(d_type)
#     db_connection.close_connection()
#     logger = get_logger()
#     logger.info(f"Unhandled types for label encoding: {unhandled}")
#
#     return label_encoders
