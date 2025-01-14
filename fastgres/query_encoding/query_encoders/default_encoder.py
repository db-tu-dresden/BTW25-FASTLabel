import abc
import datetime
import hashlib
import numpy as np

from fastgres.model.context import Context
from fastgres.query_encoding.feature_extractor import EncodingInformation
from fastgres.query_encoding.query import Query
from fastgres.baseline.log_utils import get_logger
from fastgres.query_encoding.query_encoders.encoder import Encoder

from collections import namedtuple


class FastgresDefaultEncoder:

    operator_dictionary = {
        "eq": [0, 0, 1],
        "gt": [0, 1, 0],
        "lt": [1, 0, 0],
        "lte": [1, 0, 1],
        "gte": [0, 1, 1],
        "neq": [1, 1, 0],
        "IS": [0, 0, 1],
        "in": [0, 0, 1],
        "like": [1, 1, 1]
    }

    def __init__(self, context: Context, encoding_information: EncodingInformation):
        self.context = context
        self.encoding_info = encoding_information

    def encode(self, encoding_dict: dict):
        encoded_query = list()
        merged_context = sorted(set().union(*self.context.covered_contexts))
        for table in sorted(merged_context):
            if table not in encoding_dict.keys():
                # extend by all column encodings to 0
                encoded_query.extend([0] * 4 * len(self.encoding_info.db_type_dict[table.lower()].keys()))
                continue
            column_dict = encoding_dict[table]
            for column in self.encoding_info.db_type_dict[table].keys():
                if column in column_dict:
                    entry = column_dict[column]
                    encoded_query.extend(entry)
                else:
                    encoded_query.extend([0] * 4)
        return encoded_query

    @staticmethod
    def min_max_encode(min_value, max_value, value_to_encode, offset):
        value_to_encode = np.clip(value_to_encode, min_value, max_value)
        adjusted_min = min_value - offset
        denominator = max_value - adjusted_min
        if denominator != 0:
            encoding = round((value_to_encode - adjusted_min) / denominator, 8)
        else:
            encoding = 0.0
        return encoding

    def encode_operator(self, operator):
        try:
            if operator == "not_like":
                encoded_operator = self.operator_dictionary["like"]
            else:
                encoded_operator = self.operator_dictionary[operator]
        except KeyError:
            raise KeyError("Could not encode operator: {}. Operator dictionary needs to be adjusted"
                           .format(operator))
        return encoded_operator

    def build_feature_dict_old(self, query: Query):
        encoding_dict = dict()
        db_d = self.encoding_info.db_type_dict
        mm_d = self.encoding_info.min_max_dict
        label_d = self.encoding_info.label_encoders
        skipped_d = self.encoding_info.skipped_columns
        wc_d = self.encoding_info.wildcard_dict
        for table in query.attributes:
            for column in query.attributes[table]:
                feature_vector = [0.0] * 4
                for operator in query.attributes[table][column]:
                    column_type = db_d[table.lower()][column.lower()]
                    filter_value = query.attributes[table][column][operator]
                    feature_vector[:3] = self.encode_operator(operator)

                    if isinstance(filter_value, dict):
                        # TODO: Handle if not gt offset date joins in future scenarios
                        continue

                    if column_type == "integer":
                        # some weird join in stack
                        if isinstance(filter_value, str):
                            # print("Probably some undetected joins: {} ".format(filter_value))
                            continue
                        offset = 0.001
                        # encode min max
                        min_v, max_v = mm_d[table.lower()][column.lower()]['min'], mm_d[table.lower()][column.lower()][
                            'max']
                        feature_vector[3] = self.min_max_encode(min_v, max_v, filter_value, offset)
                    elif column_type == "character varying":
                        offset = 1.0
                        if table in skipped_d:
                            if column in skipped_d[table]["columns"]:
                                max_enc = 2 ** 64  # md5 output is 64 bit standard
                                min_enc = 0  # should be min for hashes
                                offset = 1
                                if operator == "in":
                                    merged_string_hash = list()
                                    for string in filter_value:
                                        b_string = bytes(string, "utf-8")
                                        hash_v = int.from_bytes(hashlib.md5(b_string).digest()[:8], 'little')
                                        merged_string_hash.append(hash_v)
                                    hash_value = int(round(sum(merged_string_hash) / len(filter_value), 0))
                                else:
                                    b_string = bytes(filter_value, "utf-8")
                                    hash_value = int.from_bytes(hashlib.md5(b_string).digest()[:8], 'little')
                                # TODO: Counter-check skipped table encodings for expressiveness
                                feature_vector[3] = (hash_value + offset - min_enc) / (max_enc - min_enc + offset)

                                # since we continue, we need to save here
                                try:
                                    encoding_dict[table][column] = feature_vector
                                except KeyError:
                                    encoding_dict[table] = dict()
                                    encoding_dict[table][column] = feature_vector
                                continue

                        # single, ensemble, wildcard
                        if operator == "eq" or operator == "lt" or operator == "gt":
                            encoder = label_d[table][column]
                            min_v, max_v = 0, len(encoder.classes_)
                            adjusted_min = min_v - offset
                            try:
                                transformed = encoder.transform([filter_value])[0]
                            except KeyError:
                                # print("Filter error, defaulting to 0 encoding: ", filter_value)
                                transformed = min_v
                            encoded_filter_value = (transformed - adjusted_min) / \
                                                   (max_v - adjusted_min)
                            if not operator == "eq":
                                pass
                            feature_vector[3] = encoded_filter_value

                        elif operator == "in":
                            encoder = label_d[table][column]
                            min_v, max_v = 0, len(encoder.classes_)
                            adjusted_min = min_v - offset
                            try:
                                transformed = encoder.transform(filter_value)
                            except KeyError:
                                transformed = list()
                                for filter_value_i in filter_value:
                                    try:
                                        transformed.append(encoder.transform([filter_value_i])[0])
                                    except KeyError:
                                        transformed.append(-1)

                            encoded_filter_value = (np.array(transformed) - adjusted_min) / (max_v - adjusted_min)
                            encoded_filter_value = sum(encoded_filter_value) / len(encoded_filter_value)
                            feature_vector[3] = encoded_filter_value

                        elif operator == "like" or operator == "not_like":
                            try:
                                wc_dt = wc_d[table]
                                if column not in wc_dt:
                                    encoded_filter_value = 1.0
                                elif filter_value in wc_dt[column]:
                                    offset = 1.0
                                    min_v, max_v = 0, wc_dt['max']
                                    adjusted_min = min_v - offset
                                    encoded_filter_value = (wc_dt[column][filter_value] - adjusted_min) / \
                                                           (max_v - adjusted_min)
                                else:
                                    # assume that cardinalities are as high as they can get
                                    encoded_filter_value = 1.0
                            except KeyError:
                                encoded_filter_value = 1.0
                            feature_vector[3] = encoded_filter_value

                        elif operator == "neq":
                            if filter_value == '':
                                encoded_filter_value = 1.0
                            else:
                                # default eq encoding
                                encoder = label_d[table][column]
                                min_v, max_v = 0, len(encoder.classes_)
                                adjusted_min = min_v - offset
                                try:
                                    transformed = encoder.transform([filter_value])[0]
                                except KeyError:
                                    transformed = min_v
                                encoded_filter_value = (transformed - adjusted_min) / \
                                                       (max_v - adjusted_min)
                            feature_vector[3] = encoded_filter_value

                        else:
                            logger = get_logger()
                            logger.info(f"Unhandled operator: {operator}")
                    elif column_type == "timestamp without time zone":
                        # timestamps should always be caught by stc-dict
                        try:
                            # timestamps are ms-exact and the probability of having multiples is approaching 0
                            offset = datetime.timedelta(days=1)
                            # encode min max
                            min_v, max_v = mm_d[table][column]['min'], mm_d[table][column]['max']
                            format_string = "%Y-%m-%d"
                            filter_value = datetime.datetime.strptime(filter_value, format_string)
                            feature_vector[3] = self.min_max_encode(min_v, max_v, filter_value, offset)
                        except:
                            # This is a join in our scenario and can be neglected
                            continue
                    else:
                        logger = get_logger()
                        logger.info(f"Unhandled column type: {column_type}")
                    try:
                        encoding_dict[table][column] = feature_vector
                    except KeyError:
                        encoding_dict[table] = dict()
                        encoding_dict[table][column] = feature_vector
                pass
            pass
        return encoding_dict
