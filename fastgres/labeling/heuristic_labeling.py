import argparse
import os
import time
import pandas as pd
import numpy as np

from dataclasses import dataclass
from dataclasses import field
from tqdm import trange
from fastgres.baseline.database_connection import DatabaseConnection
from fastgres.definitions import PathConfig
from fastgres.workload.workload import Workload
from fastgres.baseline.log_utils import Logger, get_logger
from fastgres.baseline.utility import OperationMode as OpMode, get_one_ring_of_hint_set
from fastgres.baseline.utility import ExplainNode
from fastgres.baseline.database_connection import QueryResult
from fastgres.hinting import HintSet, HintSetFactory, get_default_library, get_available_library


@dataclass
class HintExperience:
    @dataclass
    class HintInfo:
        level: int
        hint: int

    level_dict: dict = field(default_factory=dict)

    def add_entry(self, hint_info: HintInfo, prefix: int):
        try:
            self.level_dict[hint_info.level][hint_info.hint] += prefix * 1
        except KeyError:
            try:
                self.level_dict[hint_info.level][hint_info.hint] = prefix * 1
            except KeyError:
                self.level_dict[hint_info.level] = {hint_info.hint: prefix * 1}

    def add(self, level: int, hint: int):
        hint_info = self.HintInfo(level, hint)
        self.add_entry(hint_info, prefix=1)

    def sub(self, level: int, hint: int):
        hint_info = self.HintInfo(level, hint)
        self.add_entry(hint_info, prefix=-1)

    def get_value(self, level: int, hint_int: int):
        try:
            return self.level_dict[level][hint_int]
        except KeyError:
            return 0

    def get_level(self, level: int):
        try:
            return self.level_dict[level]
        except KeyError:
            return None

    def order(self, level: int, hint_int_list: list[int]):
        occurrences = [self.get_value(level, hint_int) for hint_int in hint_int_list]
        pairs = list(zip(hint_int_list, occurrences))
        sorted_pairs = sorted(pairs, key=lambda x: x[1], reverse=True)
        return [x[0] for x in sorted_pairs]


class LabelingResult:

    def __init__(self, query_name: str, hint_set_int: int, binary_rep: list[int], measured_time: float,
                 occurred_level: int, is_opt: bool, had_timeout: bool, chosen_in_level: bool, removed: bool,
                 seen_plan: bool, hint_names: list[str]):
        self.query_name = query_name
        self.hint_set_int = hint_set_int
        self.binary_rep = binary_rep
        self.measured_time = measured_time
        self.occurred_level = occurred_level
        self.is_opt = is_opt
        self.had_timeout = had_timeout
        self.chosen_in_level = chosen_in_level
        self.removed = removed
        self.seen_plan = seen_plan
        self.hint_names = hint_names

    def __eq__(self, other):
        return (self.query_name == other.query_name
                and self.hint_set_int == other.hint_set_int
                and self.binary_rep == other.binary_rep
                and self.measured_time == other.measured_time)

    def to_dict(self) -> dict:
        # hint_names = HintSet(0).collection.get_hint_names()
        return_dict = {"query_name": self.query_name, "hint_set_int": self.hint_set_int}
        for i in range(len(self.hint_names)):
            return_dict[self.hint_names[i]] = self.binary_rep[i]
        return_dict["time"] = self.measured_time
        return_dict["level"] = self.occurred_level
        return_dict["opt"] = self.is_opt
        return_dict["timeout"] = self.had_timeout
        return_dict["chosen"] = self.chosen_in_level
        return_dict["removed"] = self.removed
        return_dict["seen_plan"] = self.seen_plan
        return return_dict


class HeuristicLabelingSettings:

    def __init__(self, query_path: str, save_path: str, config_path: str, database_string: str, use_extension: bool,
                 use_default_hints: bool, use_experience: bool, use_early_stopping: bool, use_hint_removal: bool,
                 use_level_restriction: bool, op_mode: str):

        # static settings
        self.stop_level: int = 4
        self._initial_early_stopping_threshold: int = 2
        self.early_stopping_threshold = self._initial_early_stopping_threshold
        self.early_stopping_factor = 1.1  # controls how much speedup increase we need in every level
        self._absolute_timeout: float = 500.0  # ms
        self._relative_timeout: float = 1.2  # factor

        self.query_path = query_path
        self.save_path = save_path
        self.config_path = config_path
        self.database_string = database_string
        self.use_extension = use_extension
        self.use_default_hints = use_default_hints
        self.use_experience = use_experience
        self.use_early_stopping = use_early_stopping
        self.use_hint_removal = use_hint_removal
        self.use_level_restriction = use_level_restriction
        self.op_mode = OpMode.SUB if op_mode == "sub" else OpMode.ADD

        self.use_aggressive_timeout = self.use_experience

        self.workload = Workload(self.query_path)
        self.path_config = PathConfig(self.config_path)
        self.dbc = DatabaseConnection(self.path_config.get_db_connection(self.database_string))
        _ = Logger(self.path_config, f"{os.path.basename(self.save_path)[:-4]}.log")
        self.logger = get_logger()

        if self.use_default_hints:
            used_hint_library = get_default_library()
        else:
            used_hint_library = get_available_library(self.dbc.version(), False, False, False)
        self.hs_factory = HintSetFactory(used_hint_library)
        self.hints_in_use_count = self.hs_factory.hint_library.collection_size

    def get_timeout(self, pg_default: float):
        return max(self._absolute_timeout, pg_default * self._relative_timeout)


class Labeling:

    def __init__(self, settings: HeuristicLabelingSettings):
        self.settings = settings

        self.starting_hint_set_int = 2 ** self.settings.hints_in_use_count - 1 \
            if self.settings.op_mode == OpMode.SUB else 0
        self.starting_hint_set = self.settings.hs_factory.hint_set(self.starting_hint_set_int)

        self.base_timeout = 300_000
        self.level = 0
        self.timeout = self.base_timeout

        self.experience = HintExperience()

    def label_query(self, query_name: str):
        query_results = list()
        seen_plans = dict()
        es_level = 0
        query = self.settings.workload.read_query(query_name)
        self.settings.logger.info(f"Evaluating Hint Set: {self.starting_hint_set_int}")

        # Adding query plans to seen plans
        q_plan_node = ExplainNode(self.settings.dbc.explain_query(query, self.starting_hint_set))
        self.settings.logger.info(f"Default Evaluation for query: {query_name}")
        q_result = self.settings.dbc.evaluate_hinted_query(query, self.starting_hint_set, timeout=self.base_timeout,
                                                           pre_warm=False)
        seen_plans[q_plan_node] = q_result

        labeling_result = LabelingResult(
            query_name=query_name, hint_set_int=self.starting_hint_set.hint_set_int,
            binary_rep=self.starting_hint_set.get_binary(), measured_time=q_result.time, occurred_level=self.level,
            is_opt=False, had_timeout=q_result.timed_out, chosen_in_level=True, removed=False, seen_plan=False,
            hint_names=self.settings.hs_factory.hint_library.get_hint_names()
        )
        query_results.append(labeling_result)
        self.level += 1
        current_opt = labeling_result
        self.timeout = self.settings.get_timeout(q_result.time)
        last_chosen = self.starting_hint_set_int

        neighborhood_opt = None
        neighbors = get_one_ring_of_hint_set(self.starting_hint_set_int, self.settings.hints_in_use_count,
                                             self.settings.op_mode, hint_restrictions=None)
        sorted_neighbors = list(sorted(neighbors))
        sorted_neighbors = self.experience.order(self.level, sorted_neighbors) \
            if self.experience is not None else sorted_neighbors
        hint_restrictions = set()

        while sorted_neighbors:
            hint_set_ints = [last_chosen - neighbor for neighbor in sorted_neighbors] \
                if self.settings.op_mode == self.settings.op_mode.SUB else np.add(last_chosen,
                                                                                  sorted_neighbors).tolist()
            for idx in range(len(hint_set_ints)):
                seen_plan = False
                hint_set_int = hint_set_ints[idx]
                hint_set = self.settings.hs_factory.hint_set(hint_set_int)
                self.settings.logger.info(f"Evaluating Hint Set: {hint_set_int}")
                hs_q_plan_node = ExplainNode(self.settings.dbc.explain_query(query, hint_set))
                if hs_q_plan_node in seen_plans:
                    seen_plan = True
                    hs_q_plan = seen_plans[hs_q_plan_node]
                    self.settings.logger.info(f"Observed matching hash. "
                                              f"Adding time: {hs_q_plan.time} to already observed query plan")
                    hs_result = QueryResult(query, hint_set_int, hs_q_plan.time, timeout_used=hs_q_plan.timeout_used,
                                            timed_out=hs_q_plan.timed_out, pre_warmed=False, query_plan=dict())
                else:
                    hs_result = self.settings.dbc.evaluate_hinted_query(query, hint_set, timeout=self.timeout)
                    seen_plans[hs_q_plan_node] = hs_result

                if self.settings.use_aggressive_timeout and hs_result.time < current_opt.measured_time:
                    self.timeout = self.settings.get_timeout(hs_result.time)

                remove_hint = self.settings.use_hint_removal and hs_result.timed_out
                if remove_hint:
                    self.settings.logger.info(f"Added Hint: {sorted_neighbors[idx]} to ignored hints")
                    hint_restrictions.add(sorted_neighbors[idx])
                labeling_result = LabelingResult(
                    query_name=query_name, hint_set_int=hint_set.hint_set_int, binary_rep=hint_set.get_binary(),
                    measured_time=hs_result.time, occurred_level=self.level, is_opt=False,
                    had_timeout=hs_result.timed_out, chosen_in_level=False,  removed=True if remove_hint else False,
                    seen_plan=seen_plan, hint_names=self.settings.hs_factory.hint_library.get_hint_names()
                )
                query_results.append(labeling_result)

                if neighborhood_opt is None or labeling_result.measured_time < neighborhood_opt.measured_time:
                    neighborhood_opt = labeling_result
                if labeling_result.measured_time < current_opt.measured_time:
                    current_opt = labeling_result
                    if labeling_result.measured_time * self.settings.early_stopping_factor < current_opt.measured_time:
                        es_level = self.level

                if self.settings.use_experience:
                    if hs_result.timed_out:
                        self.experience.sub(self.level, sorted_neighbors[idx])
                        self.settings.logger.info(f"Added negative experience for query: {query_name}, "
                                                  f"level: {self.level}, "
                                                  f"hint: {sorted_neighbors[idx]}")
                    else:
                        self.experience.add(self.level, sorted_neighbors[idx])

            chosen_idx = query_results.index(neighborhood_opt)
            query_results[chosen_idx].chosen_in_level = True

            if self.settings.use_early_stopping and self.level - es_level >= self.settings.early_stopping_threshold:
                self.settings.logger.info(f"Using early stopping to break at level: {self.level}")
                break
            if self.settings.use_level_restriction and self.level >= self.settings.stop_level:
                self.settings.logger.info(f"Using stop level to break: {self.level}")
                break

            new_neighbors = get_one_ring_of_hint_set(query_results[chosen_idx].hint_set_int,
                                                     self.settings.hints_in_use_count,
                                                     self.settings.op_mode, hint_restrictions=hint_restrictions)
            sorted_neighbors = list(sorted(new_neighbors))
            sorted_neighbors = self.experience.order(self.level, sorted_neighbors) \
                if self.experience is not None else sorted_neighbors
            last_chosen = neighborhood_opt.hint_set_int
            self.timeout = self.settings.get_timeout(neighborhood_opt.measured_time)  # set new level baseline

            self.level += 1
            neighborhood_opt = None

        # set opt flag
        idx = query_results.index(current_opt)
        query_results[idx].is_opt = True
        self.level = 0
        return query_results

    def label_queries(self):
        all_results = list()
        t0 = time.time()
        for query_index in trange(len(self.settings.workload.query_names)):
            query_name = self.settings.workload.query_names[query_index]
            self.settings.logger.info('Evaluating query: {}, {} / {}'.format(query_name, query_index + 1,
                                                                             len(self.settings.workload.query_names)))
            query_results = self.label_query(query_name)
            all_results.extend(query_results)
            df = pd.DataFrame([result.to_dict() for result in all_results])
            df.to_csv(self.settings.save_path, index=False)
        t1 = time.time() - t0
        self.settings.logger.info(f'Finished labeling {len(self.settings.workload.query_names)} '
                                  f'queries in {int(t1 / 60)}min {int(t1 % 60)}s.')
        return


def run():
    parser = argparse.ArgumentParser(description="Generate physical operator labels for input queries and save to json")

    parser.add_argument("queries", help="Directory in which .sql-queries are located")
    parser.add_argument("-o", "--output", required=True, help="Output csv save name")
    parser.add_argument("-c", "--config", required=True, help="Path to config file.")

    parser.add_argument("-db", "--database", required=True, choices=["imdb", "stack_overflow"], help="")
    parser.add_argument("-ud", "--use-default", action="store_true", help="Whether or not to use default (six) hints.")

    parser.add_argument("-ue", "--use-experience", action="store_true", help="Whether or not to use experience.")
    parser.add_argument("-ues", "--use-early-stopping", action="store_true", help="Whether or not to use "
                                                                                  "early stopping.")
    parser.add_argument("-uhr", "--use-hint-removal", action="store_true", help="Whether or not to use hint removal.")
    parser.add_argument("-ulr", "--use-level-restriction", action="store_true", help="Whether or not to use "
                                                                                     "level restriction.")

    parser.add_argument("-m", "--mode", default="sub", choices=["sub", "add"],
                        help="Which mode (adding/subtracting) of hints to use. This refers to enabling and disabling.")
    args = parser.parse_args()

    if not os.path.exists(args.queries):
        raise ValueError(f"Invalid query path: {args.queries}.")
    if os.path.exists(args.output):
        raise ValueError(f"Save path: {args.output} already exists.")
    if not os.path.exists(args.config):
        raise ValueError(f"Invalid config path: {args.config}.")

    settings = HeuristicLabelingSettings(args.queries, args.output, args.config, args.database, args.use_extension,
                                         args.use_default, args.use_experience, args.use_early_stopping,
                                         args.use_hint_removal, args.use_level_restriction, args.mode)
    # initial considerations
    settings.dbc.disable_geqo()
    settings.logger.info(f"\nRunning Labeling on:\n {settings.dbc.version()}.\n")

    try:
        labeling = Labeling(settings)
        t0 = time.time()
        labeling.label_queries()
        labeling_time = time.time() - t0
        settings.logger.info(f"Finished Label Generation in: {round(labeling_time, 2)}s")
    except KeyboardInterrupt:
        settings.dbc.close_connection()


if __name__ == "__main__":
    run()
