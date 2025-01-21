import argparse
import dataclasses
import os
import numpy as np
import pandas as pd

from tqdm import tqdm

from fastgres.baseline.utility import ExplainNode
from fastgres.baseline.log_utils import Logger, get_logger
from fastgres.workload.workload import Workload
from fastgres.definitions import PathConfig
from fastgres.baseline.database_connection import DatabaseConnection, QueryResult

from fastgres.hinting import HintSetFactory, get_default_library, get_available_library


@dataclasses.dataclass
class Result:
    query_name: str
    hint_set_int: int
    time: float
    timed_out: bool


class LabelConfig:
    def __init__(self, query_path: str, archive_path: str, config_path: str, save_path: str, database_name: str,
                 use_default: bool):
        self.query_path = query_path
        self.archive_path = archive_path
        self.config_path = config_path
        self.save_path = save_path
        self.database_name = database_name

        self.workload = Workload(self.query_path)
        self.log_name = os.path.basename(self.save_path)[:-4]  # truncate file extension .csv
        self.path_config = PathConfig(self.config_path)
        _ = Logger(self.path_config, f"{os.path.basename(self.save_path)[:-4]}.log")
        self.logger = get_logger()
        self.dbc = DatabaseConnection(self.path_config.get_db_connection(self.database_name), self.database_name)
        self.use_default = use_default
        if self.use_default:
            used_hint_library = get_default_library()
        else:
            used_hint_library = get_available_library(self.dbc.version(), False, False, False)
        self.hs_factory = HintSetFactory(used_hint_library)
        self.used_hints_count = self.hs_factory.hint_library.collection_size
        self.dbc.disable_geqo()
        self.logger.info(f"Using: {self.used_hints_count} hints")


def label_results(config: LabelConfig):
    default_hint_set_int = 2 ** config.used_hints_count - 1
    config.logger.info(f"Default hint set: {default_hint_set_int}")
    result_df = pd.read_csv(config.archive_path)
    to_label_df = result_df[result_df["test_query"] == True]

    unique_queries = np.unique(to_label_df["query_name"].tolist())
    results = list()
    for query_name in tqdm(unique_queries, "Labeling Queries"):
        config.logger.info(f"Labeling query: {query_name}")
        seen_plans = dict()
        query = config.workload.read_query(query_name)
        query_df = to_label_df[to_label_df["query_name"] == query_name]
        unique_labels = np.unique(query_df["label"].tolist())
        unique_predictions = np.unique(query_df["prediction"].tolist())
        hint_set_ints_to_label = set(unique_labels).union(set(unique_predictions))

        default_hint_set = config.hs_factory.hint_set(default_hint_set_int)
        config.logger.info(f"Explain for Query: {query_name} using query: {query}")
        q_plan_node = ExplainNode(config.dbc.explain_query(query, default_hint_set))
        default_result = config.dbc.evaluate_hinted_query(query, default_hint_set, timeout=150_000)
        seen_plans[q_plan_node] = default_result

        label_timeout = 1.1 * default_result.time
        timeout_extra_time = 2.1 * default_result.time
        result = Result(query_name, default_hint_set_int, default_result.time, default_result.timed_out)
        results.append(result)

        for hint_set_int in tqdm(hint_set_ints_to_label, "Hints to label"):
            hint_set = config.hs_factory.hint_set(int(hint_set_int))
            hs_q_plan_node = ExplainNode(config.dbc.explain_query(query, hint_set))
            if hs_q_plan_node in seen_plans:
                hs_q_plan = seen_plans[hs_q_plan_node]
                config.logger.info(f"Observed matching hash. "
                                   f"Adding time: {hs_q_plan.time} to already observed query plan for "
                                   f"query: {query_name} and hint set: {hint_set_int}")
                hint_set_result = QueryResult(query, hint_set_int, hs_q_plan.time, timeout_used=hs_q_plan.timeout_used,
                                              timed_out=hs_q_plan.timed_out, pre_warmed=False, query_plan=dict())
            else:
                hint_set_result = config.dbc.evaluate_hinted_query(query, hint_set, timeout=label_timeout)
                seen_plans[hs_q_plan_node] = hint_set_result

            '''adding timeout_extra_time is equivalent to breaking after 1.1*timeout 
             then executing the query normally'''
            hs_result = Result(query_name, hint_set_int,
                               timeout_extra_time if hint_set_result.timed_out else hint_set_result.time,
                               hint_set_result.timed_out)
            results.append(hs_result)
        res_df = pd.DataFrame([res.__dict__ for res in results])
        res_df.to_csv(config.save_path, index=False)
    return results


def run():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("queries", type=str, help="<Path/to/queries/>")
    parser.add_argument("-a", "--archive", type=str, help="<Prediction/archive/to/label.csv>")
    parser.add_argument("-o", "--output", default=None, help="<Labeled/output/path.csv>")
    parser.add_argument("-c", "--config", default=None, help="<Database_config_path/>")
    parser.add_argument("-db", "--database", choices=["imdb", "stack_overflow"], help="Database that was used.")
    parser.add_argument("-ud", "--use-default", action="store_true", help="Whether or not to use default (six) hints.")
    args = parser.parse_args()

    if not os.path.exists(args.queries):
        raise argparse.ArgumentError(args.queries, "Query path does not exist.")
    if os.path.exists(args.output):
        raise argparse.ArgumentError(args.output, "Save path already exists.")
    if not os.path.exists(args.archive):
        raise argparse.ArgumentError(args.archive, "Archive path does not exist.")

    config = LabelConfig(args.queries, args.archive, args.config, args.output, args.database, args.use_default)
    results = label_results(config)
    # final save to be sure
    res_df = pd.DataFrame([res.__dict__ for res in results])
    res_df.to_csv(config.save_path, index=False)


if __name__ == "__main__":
    run()
