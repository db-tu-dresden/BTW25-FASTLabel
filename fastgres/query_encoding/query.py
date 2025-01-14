
from mo_sql_parsing import parse
from fastgres.model.context import Context
from fastgres.workload.workload import Workload
from fastgres.baseline.log_utils import get_logger
from fastgres.query_encoding.query_handlers.select_handler import get_select
from fastgres.query_encoding.query_handlers.equality_handler import get_equality_information
from fastgres.query_encoding.query_handlers.context_handler import get_table_entries
from fastgres.query_encoding.query_handlers.like_handler import get_like_information
from fastgres.query_encoding.query_handlers.greater_lesser_than_handler import get_gtlt_information
from fastgres.query_encoding.query_handlers.not_equal_handler import get_neq_information
from fastgres.query_encoding.query_handlers.not_like_handler import get_not_like_information
from fastgres.query_encoding.query_handlers.in_handler import get_in_information
from fastgres.query_encoding.query_handlers.greater_lesser_than_or_equal_handler import get_gtlte_information


class Query:

    def __init__(self, *args):
        if isinstance(args[0], str) and isinstance(args[1], Workload) and len(args) == 2:
            self.name = args[0]
            self.parsed = args[1].parse_query(self.name)
        elif isinstance(args[0], str) and isinstance(args[1], str) and len(args) == 2:
            try:
                self.name = args[0]
                self.parsed = parse(args[1])
            except:
                logger = get_logger()
                logger.info(f"Parsing error for query: {args[0]}, {args[1]}")
                raise ValueError("Could not parse query")
        elif isinstance(args[0], str) and isinstance(args[1], dict) and len(args) == 2:
            self.name = args[0]
            self.parsed = args[1]
        else:
            raise ValueError(f"Could not initialize Query with arguments: {args}")

        self._select = None
        # TODO: handle separately
        self.from_part = self.parsed['from']
        self.where_part = self.parsed['where']

        self.unhandled = set()

        self._tables = None
        self._context = None

        # actual encoding information
        self._attributes = None

    @property
    def select(self):
        if self._select is None:
            self._select = get_select(self.parsed)
        return self._select

    @property
    def tables(self):
        if self._tables is None:
            self._tables = get_table_entries(self.from_part)
        return self._tables

    @property
    def context(self):
        if self._context is None:
            self._context = frozenset(sorted(self.tables.values()))
        return self._context

    @property
    def attributes(self):
        if self._attributes is None:
            self._attributes = self.get_attributes()
        return self._attributes

    def __eq__(self, other):
        return self.parsed == other.parsed and self.attributes == other.attributes

    def to_dict(self):
        return {
            "name": self.name,
            "parsed": self.parsed,
            "select": self.select,
            "unhandled": list(self.unhandled),
            "tables": self.tables,
            "context": list(self.context),
            "attributes": self.attributes
        }

    @classmethod
    def from_dict(cls, query_dict):
        obj = cls(query_dict["name"], query_dict["parsed"])
        obj.parsed = query_dict["parsed"]
        obj._select = query_dict["select"]
        obj.from_part = obj.parsed["from"]
        obj.where_part = obj.parsed["where"]
        obj.unhandled = set(query_dict["unhandled"])

        obj._tables = query_dict["tables"]
        obj._context = frozenset(sorted(query_dict["context"]))
        obj._attributes = query_dict["attributes"]
        return obj

    def __gt__(self, other):
        return self.name > other.name

    def __lt__(self, other):
        return self.name < other.name

    def is_in(self, context: Context):
        return self.context in context.covered_contexts

    def print_info(self):
        print(self.name)
        print('Context: ', self.context)
        print('Unhandled Operators: ', self.unhandled)
        print('\n')
        return

    def get_attributes(self) -> dict:
        # table -> column -> key -> value to encode
        attribute_dict = dict()

        combination_types = self.where_part.keys()
        if len(combination_types) > 1 and 'and' not in combination_types:
            raise ValueError('Encountered unhandled combinator unequal to - and -')

        # sometimes there might not be more than one where argument
        try:
            and_part = self.where_part['and']
        except KeyError:
            and_part = [self.where_part]

        for entry in and_part:
            alias, column, key, value = [None] * 4
            entry_keys = entry.keys()

            if 'eq' in entry_keys:
                alias, column, key, value = get_equality_information(entry)
            elif 'like' in entry_keys:
                alias, column, key, value = get_like_information(entry)
            elif 'gt' in entry_keys or 'lt' in entry_keys:
                alias, column, key, value = get_gtlt_information(entry, entry_keys)
            elif 'neq' in entry_keys:
                alias, column, key, value = get_neq_information(entry)
            elif 'not_like' in entry_keys:
                alias, column, key, value = get_not_like_information(entry)
            elif 'exists' in entry_keys:
                key = 'exists'
                exists_statement = entry[key]
                pass
            elif 'between' in entry_keys:
                key = 'between'
                between_statement = entry[key]
            elif 'in' in entry_keys:
                alias, column, key, value = get_in_information(entry)
            elif 'missing' in entry_keys:
                key = 'missing'
                missing_statement = entry[key]
            # handle disjunctive keys inside conjunctive ones
            elif 'or' in entry_keys:
                key = 'or'
                or_statement = entry[key]
                # disjunctive queries are not yet supported
                pass
            elif 'gte' in entry_keys or 'lte' in entry_keys:
                alias, column, key, value = get_gtlte_information(entry, entry_keys)
            else:
                [self.unhandled.add(i) for i in entry_keys]
                pass

            # final encoding information
            if column is None:
                pass
            else:
                table = self.tables[alias]
                try:
                    attribute_dict[table][column][key] = value
                except KeyError:
                    try:
                        attribute_dict[table][column] = dict()
                        attribute_dict[table][column][key] = value
                    except:
                        try:
                            attribute_dict[table] = dict()
                            attribute_dict[table][column] = dict()
                            attribute_dict[table][column][key] = value
                        except:
                            logger = get_logger()
                            logger.info(f"Error saving query information in attribute dict {attribute_dict}")
                            raise ValueError()

        return attribute_dict
