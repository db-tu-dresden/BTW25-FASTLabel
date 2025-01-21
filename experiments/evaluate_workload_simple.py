import argparse
import dataclasses
import os
import time

import numpy as np
import pandas as pd

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from tqdm import tqdm
from joblib.parallel import Parallel, delayed

from fastgres.query_encoding.encoded_query import EncodedQuery
from fastgres.labeling.archive import DataframeArchive
from fastgres.workload.workload import Workload
from fastgres.baseline.database_connection import DatabaseConnection
from fastgres.definitions import PathConfig
from fastgres.model.context import Context
from fastgres.query_encoding.query import Query
from fastgres.baseline.utility import save_json, load_json, set_seeds
from fastgres.baseline.log_utils import Logger, get_logger
from fastgres.query_encoding.feature_extractor import EncodingInformation


@dataclasses.dataclass
class FastgresResult:
    query_name: str
    prediction: int
    label: int
    default_time: float
    opt_time: float
    context: set[frozenset]
    train_query: bool
    test_query: bool


class FastgresSettings:

    def __init__(self, query_path: str, save_path: str, config_path: str, archive_path: str, database_name: str,
                 train_size: float, execute_queries: bool, use_contexts: bool, seed: int, database_statistics_path: str,
                 encoded_query_path: [str, None]):
        self.query_path = query_path
        self.save_path = save_path
        self.config_path = config_path
        self.archive_path = archive_path
        self.database_name = database_name
        self.train_size = train_size
        self.execute_queries = execute_queries
        self.use_contexts = use_contexts
        self.seed = seed
        self.database_statistics_path = database_statistics_path
        self.encoded_query_path = encoded_query_path

        self.workload = Workload(self.query_path)
        self.log_name = os.path.basename(self.save_path)[:-4]  # truncate file extension .csv
        self.path_config = PathConfig(self.config_path)
        _ = Logger(self.path_config, f"{os.path.basename(self.save_path)[:-4]}.log")
        self.logger = get_logger()
        self.dbc = DatabaseConnection(self.path_config.get_db_connection(self.database_name), self.database_name)
        self.archive = DataframeArchive(self.archive_path)

        if self.encoded_query_path is not None:
            self.logger.info("Loading pre-encoded queries.")
            loaded_encoded_queries = load_json(self.encoded_query_path)
            self.pre_encoded_queries = dict()
            for query_name, query_dict in loaded_encoded_queries.items():
                q = Query.from_dict(query_dict)
                self.pre_encoded_queries[query_name] = q
            self.logger.info(f"Loaded {len(loaded_encoded_queries)} pre-encoded queries successfully.")
        else:
            self.logger.info("Did not load pre-encoded queries.")
            self.pre_encoded_queries = dict()
        try:
            self.encoding_info = EncodingInformation(self.dbc, self.database_statistics_path, self.workload)
        except Exception as e:
            self.encoding_info = EncodingInformation(self.dbc, self.database_statistics_path, self.workload,
                                                     eager_load=False)
            self.encoding_info.build_encoding_info()
        self.context_queries = self._classify_queries()

    def _classify_queries(self):
        self.logger.info(f"Classifying Queries into contexts: {self.use_contexts}")
        context_dict = dict()
        if self.use_contexts:
            for query_name in tqdm(self.workload.query_names, desc="Classifying Queries"):
                if self.pre_encoded_queries:
                    query = self.pre_encoded_queries[query_name]
                else:
                    query = Query(query_name, self.workload)
                context = Context(query.context)
                if context in context_dict:
                    context_dict[context].append(query)
                else:
                    context_dict[context] = [query]
        else:
            merged_context = Context()
            queries = list()
            for query_name in tqdm(self.workload.query_names, desc="Classifying Queries"):
                if self.pre_encoded_queries:
                    query = self.pre_encoded_queries[query_name]
                else:
                    query = Query(query_name, self.workload)
                if query.context not in merged_context.covered_contexts:
                    merged_context.add_context(query.context)
                queries.append(query)
            context_dict[merged_context] = queries
        self.logger.info(f"Using {len(context_dict)} contexts")
        return context_dict

    def prepare_input(self):

        def prep_context_input(context):
            context_queries = self.context_queries[context]
            context_labels = np.array([self.archive.get_opt(query.name) for query in context_queries])

            if self.train_size == 1.0:
                return context, context_queries, context_queries, context_labels, context_labels

            context_train_queries, context_test_queries, context_train_labels, context_test_labels = train_test_split(
                context_queries, context_labels, train_size=self.train_size, random_state=self.seed
            )
            return context, context_train_queries, context_test_queries, context_train_labels, context_test_labels

        train_queries, test_queries, train_labels, test_labels = [dict() for _ in range(4)]
        # parallelize
        results = Parallel(n_jobs=len(self.context_queries.keys()), prefer="threads")(
            delayed(prep_context_input)(context) for context in self.context_queries
        )

        # collect
        for context, c_train_q, c_test_q, c_train_l, c_test_l in results:
            train_queries[context] = c_train_q
            test_queries[context] = c_test_q
            train_labels[context] = c_train_l
            test_labels[context] = c_test_l

        return train_queries, test_queries, train_labels, test_labels

    def featurize(self, context_queries: dict[Context, list[Query]]):
        context_features = dict()
        for context in tqdm(context_queries, desc="Featurizing Context"):
            queries = context_queries[context]
            query_features = list()
            t0 = time.perf_counter_ns()
            for query in tqdm(queries, desc="Featurizing Queries"):
                encoded_query = EncodedQuery(context, query, self.encoding_info)
                query_features.append(encoded_query.encoded_query)
            t1 = (time.perf_counter_ns() - t0) / 1_000_000_000
            self.logger.info(f"Featurization Default Query Order: {query_features[:5]} in {t1}s")
            context_features[context] = query_features
        return context_features


class IntegerModel:

    def __init__(self):
        self._model_integer = None

    def fit(self, labels: list[int]):
        self._model_integer = labels[0]
        return self

    def predict(self, features):
        return [self._model_integer for _ in features]


class Model:

    def __init__(self):
        self._model = None

    def fit(self, features, labels, seed):
        if len(np.unique(labels)) <= 1:
            int_model = IntegerModel()
            return int_model.fit(labels)
        self._model = GradientBoostingClassifier(max_depth=1000, random_state=seed).fit(features, labels)
        return self._model

    def predict(self, features):
        if isinstance(self._model, IntegerModel):
            return self._model.predict(features)
        return int(self._model.predict(np.reshape(features, (1, -1)))[0])


def train_models(context_train_features, context_train_labels, seed):
    def train_model(context, context_train_features, context_train_labels):
        features = context_train_features[context]
        tqdm.write(f"Training on: {len(features)} queries")
        return context, Model().fit(features, context_train_labels[context], seed)

    models_with_contexts = (Parallel(n_jobs=-1, prefer="threads")(
        delayed(train_model)
        (context, context_train_features, context_train_labels)
        for context in context_train_features)
    )
    context_models = {context: model for context, model in models_with_contexts}
    return context_models


def test_models(context_test_features, context_models):
    predictions = dict()
    for context in context_test_features:
        model = context_models[context]
        predictions[context] = model.predict(context_test_features[context])
    return predictions


def run():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("queries", default=None, help="<Path/to/queries/>")
    parser.add_argument("-o", "--output", default=None, help="<Output/path.csv>")
    parser.add_argument("-c", "--config", default=None, help="<Database_config_path/>")
    parser.add_argument("-db", "--database", choices=["imdb", "stack_overflow"], help="Database that was used.")
    parser.add_argument("-a", "--archive", default=None, help="<Path/to/archive.csv>. The archive is retrieved "
                                                              "through labeling.")
    parser.add_argument("-exec", "--execute-queries", action="store_true", help="Unused right now.")
    parser.add_argument("-uc", "--use-contexts", action="store_true", help="Use FG contextualization. "
                                                                           "If disabled, one global context is used.")
    parser.add_argument("-s", "--seed", type=int, default=47, help="Seed to use for reproducibility.")
    parser.add_argument("-ts", "--train-size", type=float, required=True, help="Training size to use from "
                                                                               "the given queries.")
    parser.add_argument("-stats", "--statistics", default=None, help="<Path/to/statistics/>")
    parser.add_argument("-ecp", "--encoded-query-path", default=None, help="Optional: <path/to/pre-encoded/"
                                                                           "queries.json")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        raise ValueError(f"Unknown config path: {args.config}")
    if os.path.exists(args.output):
        raise ValueError(f"Save path: {args.output} already exists.")
    if not os.path.exists(args.queries):
        raise ValueError(f"Query path: {args.queries} does not exist.")
    if not os.path.exists(args.archive):
        raise ValueError(f"Archive path: {args.archive} does not exist.")
    if not os.path.exists(args.statistics):
        raise ValueError(f"Database statistic path: {args.statistics} does not exist.")
    if args.encoded_query_path is not None and not os.path.exists(args.encoded_query_path):
        raise ValueError(f"Encoded query path: {args.encoded_query_path} does not exist.")

    '''Set all seeds for reproducibility'''
    set_seeds(args.seed)

    '''Set arguments for easier access'''
    fastgres_settings = FastgresSettings(args.queries, args.output, args.config, args.archive, args.database,
                                         args.train_size, args.execute_queries, args.use_contexts, args.seed,
                                         args.statistics, args.encoded_query_path)

    '''Preparation Phase'''
    t0 = time.time_ns()
    context_train_queries, context_test_queries, context_train_labels, context_test_labels = (
        fastgres_settings.prepare_input())
    context_train_features = fastgres_settings.featurize(context_train_queries)
    prep_time = (time.time_ns() - t0) / 1_000_000  # ms is enough
    print(f"Finished preparation in: {prep_time / 1_000}s")

    '''Training Phase'''
    t0 = time.time_ns()
    trained_models = train_models(context_train_features, context_train_labels, fastgres_settings.seed)
    train_time = (time.time_ns() - t0) / 1_000_000
    print(f"Finished training in: {train_time / 1_000}s")

    '''Testing Phase'''
    t0 = time.time_ns()
    context_test_features = fastgres_settings.featurize(context_test_queries)
    predictions = test_models(context_test_features, trained_models)
    test_time = (time.time_ns() - t0) / 1_000_000
    print(f"Finished testing in: {test_time / 1_000}s")

    '''Writing Results'''
    query_results = list()

    '''Prepare query results'''
    for context in context_train_queries:
        for q_idx in range(len(context_train_queries[context])):
            query = context_train_queries[context][q_idx]
            label = context_train_labels[context][q_idx]
            prediction = label
            train_query = True
            test_query = not train_query
            default_time = fastgres_settings.archive.get_default_time(query.name)
            opt_time = fastgres_settings.archive.get_opt_time(query.name)
            fg_res = FastgresResult(query.name, prediction, label, default_time, opt_time,
                                    context.covered_contexts, train_query, test_query)
            query_results.append(fg_res)
    for context in context_test_queries:
        for q_idx in range(len(context_test_queries[context])):
            query = context_test_queries[context][q_idx]
            label = context_test_labels[context][q_idx]
            prediction = predictions[context][q_idx]
            train_query = False
            test_query = not train_query
            default_time = fastgres_settings.archive.get_default_time(query.name)
            opt_time = fastgres_settings.archive.get_opt_time(query.name)
            fg_res = FastgresResult(query.name, prediction, label, default_time, opt_time,
                                    context.covered_contexts, train_query, test_query)
            query_results.append(fg_res)

    '''Save query results'''
    result_df = pd.DataFrame([query_res.__dict__ for query_res in query_results])
    result_df.to_csv(fastgres_settings.save_path, index=False)

    '''Prepare timing results'''
    overall_results = {"preparation_time": prep_time, "training_time": train_time, "testing_time": test_time,
                       "used_archive": args.archive}

    '''Save timing results'''
    # truncated csv + json ending
    save_json(overall_results,
              os.path.dirname(fastgres_settings.save_path) + "/" + fastgres_settings.log_name + ".json")


if __name__ == '__main__':
    run()
