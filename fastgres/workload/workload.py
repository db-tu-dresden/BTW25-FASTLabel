
import os
from mo_sql_parsing import parse
from fastgres.baseline.log_utils import get_logger
from tqdm import tqdm
from sklearn.model_selection import train_test_split


class Workload:
    def __init__(self, path: str):
        self.path: str = path
        self._query_names: list[str] = None
        self._queries: list[str] = None

    def read_query(self, query_name: str):
        with open(self.path + query_name, encoding='utf-8') as file:
            query = file.read()
        return query

    def _get_queries(self):
        queries = list()
        for file in tqdm(os.scandir(self.path), desc="Loading Queries"):
            if os.path.isfile(os.path.join(self.path, file.name)):
                if file.name.endswith('sql'):
                    queries.append(file.name)
        return list(sorted(queries))

    @property
    def queries(self):
        if self._queries is None:
            self._queries = [self.read_query(q) for q in self._get_query_names()]
        return self._queries

    @property
    def query_names(self):
        if self._query_names is None:
            self._query_names = self._get_query_names()
        return self._query_names

    def _get_query_names(self):
        queries = list()
        for file in os.scandir(self.path):
            if os.path.isfile(os.path.join(self.path, file.name)):
                if file.name.endswith('sql'):
                    queries.append(file.name)
        return queries

    def parse_query(self, query_name: str):
        with open(self.path + query_name, encoding='utf-8') as file:
            q = file.read()
        try:
            parsed_query = parse(q)
        except:
            logger = get_logger()
            logger.info(f"Parsing error for query: {query_name}")
            raise ValueError('Could not parse query')
        return parsed_query

    def split_query_names(self, train_size: float, seed: int):
        return train_test_split(self.query_names, train_size=train_size, random_state=seed)
