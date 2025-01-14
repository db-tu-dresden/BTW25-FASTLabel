
import abc
from fastgres.query_encoding.query import Query
from typing import Optional


class Encoder(abc.ABC):

    def __init__(self, query: Query):
        self.query = query

    @abc.abstractmethod
    def encode(self):
        raise NotImplementedError
