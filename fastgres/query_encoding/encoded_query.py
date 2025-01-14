
from fastgres.model.context import Context
from fastgres.query_encoding.query import Query
from fastgres.query_encoding.feature_extractor import EncodingInformation
from fastgres.query_encoding.query_encoders.default_encoder import FastgresDefaultEncoder


class EncodedQuery:

    def __init__(self, context: Context, query: Query, encoding_information: EncodingInformation):
        self.context = context
        self.query = query
        self.enc_info = encoding_information
        self._encoded_query = None

    @property
    def encoded_query(self):
        if self._encoded_query is None:
            encoder = FastgresDefaultEncoder(self.context, self.enc_info)
            self._encoded_query = encoder.encode(encoder.build_feature_dict_old(self.query))
        return self._encoded_query
