import itertools
import json
import pickle
import joblib
import enum
import math
import numpy as np
import random

from typing import Any


class OperationMode(enum.Enum):
    SUB = 1
    ADD = 0


def load_json(path: str) -> Any:
    with open(path, 'r') as file:
        loaded = json.load(file)
    return loaded


def save_json(to_save: Any, path: str) -> None:
    json_dict = json.dumps(to_save)
    with open(path, 'w') as f:
        f.write(json_dict)
    return


def load_pickle(path: str) -> Any:
    with open(path, 'rb') as file:
        loaded = pickle.load(file)
    return loaded


def save_pickle(to_save: Any, path: str) -> None:
    with open(path, 'wb') as f:
        pickle.dump(to_save, f)
    return


def load_joblib(path: str) -> Any:
    return joblib.load(path)


def save_joblib(to_save: Any, path: str) -> None:
    joblib.dump(to_save, path)
    return


def binary_to_int(bin_list: list[int]) -> int:
    return int("".join(str(x) for x in bin_list), 2)


def int_to_binary(integer: int, bin_size: int) -> list[int]:
    return list(reversed([int(i) for i in bin(integer)[2:].zfill(bin_size)]))


# https://stackoverflow.com/a/312464
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def get_one_ring_of_hint_set(hint_set_int: int, hints_count: int, op_mode: OperationMode = OperationMode.SUB,
                             hint_restrictions: set[int] = None) -> list[int]:
    """
    Calculates the neighborhood of a hint set that is reachable by switching off/on one hint
    :param hint_set_int: integer from which to spread from
    :param hints_count: amount of hints used. this variable is used to transform into proper binary representations
    :param op_mode: operation method, which steers if hints should be added or deleted to obtain the neighborhood
    :param hint_restrictions: hints to not use when considering one rings
    :return: hint set integers, representing the neighborhood sorted ascending
    """

    if hint_set_int > 2 ** hints_count - 1:
        raise ValueError(f"hint set integer is bigger than value: {2 ** hints_count - 1} defined by hint set count")
    if hint_set_int < 0:
        raise ValueError(f"Hint set value: {hint_set_int} is negative")

    if hint_restrictions is not None:
        if not all([(hint_restriction & (hint_restriction - 1) == 0) and hint_restriction != 0
                    for hint_restriction in hint_restrictions]):
            raise ValueError(f"Hint Restrictions: {hint_restrictions} are not valid")
        if op_mode == OperationMode.SUB:
            # subtracting only if needed --> a & ~b
            hint_set_int &= ~sum(hint_restrictions)
        else:
            # adding if not present --> a | b
            hint_set_int |= sum(hint_restrictions)

    if op_mode == OperationMode.ADD:
        # invert the value to calculate neighbors
        max_value = (1 << hints_count) - 1
        hint_set_int = max_value - hint_set_int

    one_ring_sorted = []
    i = 1
    # wander bitwise and add if matched
    while i <= hint_set_int:
        if i & hint_set_int:
            one_ring_sorted.append(i)
        i <<= 1

    return one_ring_sorted


class ExplainNode:
    """
    Based on: https://github.com/rbergm/PostBOUND/
    """
    def __init__(self, explain_data: dict) -> None:
        self.node_type = explain_data.get("Node Type", None)

        self.cost = explain_data.get("Total Cost", math.nan)
        self.cardinality_estimate = explain_data.get("Plan Rows", math.nan)
        self.execution_time = explain_data.get("Actual Total Time", math.nan) / 1_000
        self.true_cardinality = explain_data.get("Actual Rows", math.nan)
        self.loops = explain_data.get("Actual Loops", 1)

        self.relation_name = explain_data.get("Relation Name", None)
        self.relation_alias = explain_data.get("Alias", None)
        self.index_name = explain_data.get("Index Name", None)
        self.subplan_name = explain_data.get("Subplan Name", None)
        self.cte_name = explain_data.get("CTE Name", None)

        self.filter_condition = explain_data.get("Filter", None)
        self.index_condition = explain_data.get("Index Cond", None)
        self.join_filter = explain_data.get("Join Filter", None)
        self.hash_condition = explain_data.get("Hash Cond", None)
        self.recheck_condition = explain_data.get("Recheck Cond", None)

        self.parent_relationship = explain_data.get("Parent Relationship", None)
        self.parallel_workers = explain_data.get("Workers Launched", math.nan)

        self.shared_blocks_read = explain_data.get("Shared Read Blocks", math.nan)
        self.shared_blocks_cached = explain_data.get("Shared Hit Blocks", math.nan)
        self.temp_blocks_read = explain_data.get("Temp Read Blocks", math.nan)
        self.temp_blocks_written = explain_data.get("Temp Written Blocks", math.nan)

        self.children = [ExplainNode(child) for child in explain_data.get("Plans", [])]

        self.explain_data = explain_data
        self._hash_val = hash((self.node_type,
                               self.relation_name, self.relation_alias, self.index_name, self.subplan_name,
                               self.cte_name,
                               self.filter_condition, self.index_condition, self.join_filter, self.hash_condition,
                               self.recheck_condition,
                               self.parent_relationship, self.parallel_workers,
                               tuple(self.children)))

    def __hash__(self):
        return self._hash_val

    def __eq__(self, other):
        return self._hash_val == other._hash_val


def get_hint_set_combinations(flexible_hints: list[int], number_of_hints: int = 6) -> list[int]:
    """
    :param flexible_hints: hints which are marked to be traversed (able to be switched on or off)
    :param number_of_hints: search space limitation
    :return: hint sets in integer form, which can be traversed in the current search space and current flexible hints
    """
    # for easily mapping to integer hints by condition
    temp = {1: True, 0: False}
    # repeat decides how long the final combinations are
    bin_comb = list(itertools.product([0, 1], repeat=len(flexible_hints)))
    # same but with boolean values now
    bool_comb = [[temp[_[i]] for i in range(len(_))] for _ in bin_comb]
    combinations = list()
    cap = (2 ** number_of_hints) - 1
    # now reduce the combinations to integer values that can be used for hinting
    for comb in bool_comb:
        combinations.append(cap - sum(np.array(flexible_hints)[np.array(comb)]))
    return combinations


def set_seeds(seed: int):
    random.seed(seed)
    np.random.seed(seed)
    return
