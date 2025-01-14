
import json
import warnings
from datetime import date, datetime

from typing import Optional
from fastgres.baseline.database_connection import DatabaseConnection


class MinMaxDictionary:

    def __init__(self, mm_dict: Optional[dict] = None):
        self.min_max_dictionary = dict() if mm_dict is None else mm_dict

    def build_min_max_dict(self, db_connection: DatabaseConnection):
        tables = db_connection.tables
        for table in tables:
            col_dict = dict()
            columns_and_types = db_connection.get_columns_and_types(table)
            for column, d_type in columns_and_types:
                if d_type in ['integer', 'timestamp without time zone', 'date', 'numeric']:
                    min_v, max_v = db_connection.get_min_max(column, table)
                    col_dict[column] = dict()
                    col_dict[column]['min'] = min_v
                    col_dict[column]['max'] = max_v
                    col_dict[column]['type'] = d_type
                elif d_type in ["character varying", "text"]:
                    pass
                else:
                    warnings.warn(f"Unknown min-max encodable column type {d_type}")
            self.min_max_dictionary[table] = col_dict
        db_connection.close_connection()
        return self.min_max_dictionary

    def to_dict(self):
        return json.dumps(self.min_max_dictionary, cls=self.MinMaxEncoder)

    @classmethod
    def from_dict(cls, dictionary):
        obj = cls()
        obj.min_max_dictionary = json.loads(dictionary, cls=obj.MinMaxDecoder)
        return obj

    class MinMaxEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            return super().default(obj)

    class MinMaxDecoder(json.JSONDecoder):
        def __init__(self, *args, **kwargs):
            super().__init__(object_hook=self.object_hook, *args, **kwargs)

        def object_hook(self, obj):
            for key, value in obj.items():
                if isinstance(value, str):
                    try:
                        if 'T' in value:
                            obj[key] = datetime.fromisoformat(value)
                        else:
                            obj[key] = date.fromisoformat(value)
                    except ValueError:
                        pass
            return obj
