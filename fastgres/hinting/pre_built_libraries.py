from __future__ import annotations
from collections import namedtuple
from fastgres.hinting import HintLibrary, Hint


def reindex(input_tuples: list[PostgresHint]):
    new_input = list()
    for idx in range(len(input_tuples)):
        name, index, instruction, value = input_tuples[idx]
        new_input.append(PostgresHint(name, idx, instruction, value))
    return new_input


PostgresHint = namedtuple('PostgresHint', ['name', 'index', 'instruction', 'value'])

# pg12
INDEX_ONLY_SCAN = PostgresHint("INDEX_ONLY_SCAN", 0, "enable_indexonlyscan", True)
SEQ_SCAN = PostgresHint("SEQ_SCAN", 1, "enable_seqscan", True)
INDEX_SCAN = PostgresHint("INDEX_SCAN", 2, "enable_indexscan", True)
NESTED_LOOP_JOIN = PostgresHint("NESTED_LOOP_JOIN", 3, "enable_nestloop", True)
MERGE_JOIN = PostgresHint("MERGE_JOIN", 4, "enable_mergejoin", True)
HASH_JOIN = PostgresHint("HASH_JOIN", 5, "enable_hashjoin", True)
TID_SCAN = PostgresHint("TID_SCAN", 6, "enable_tidscan", True)
SORT = PostgresHint("SORT", 7, "enable_sort", True)
PARALLEL_HASH = PostgresHint("PARA_HASH", 8, "enable_parallel_hash", True)
PARALLEL_APPEND = PostgresHint("PARA_APPEND", 9, "enable_parallel_append", True)
MATERIALIZATION = PostgresHint("MATERIALIZATION", 10, "enable_material", True)
HASH_AGGREGATION = PostgresHint("HASH_AGG", 11, "enable_hashagg", True)
GATHER_MERGE = PostgresHint("GATHER_MERGE", 12, "enable_gathermerge", True)
BITMAP_SCAN = PostgresHint("BITMAP_SCAN", 13, "enable_bitmapscan", True)

# pg13
INCREMENTAL_SORT = PostgresHint("INC_SORT", 14, "enable_incremental_sort", True)

# pg14
MEMOIZE = PostgresHint("MEMOIZE", 15, "enable_memoize", True)

# pg15
# pg16
PRESORTED_AGGREGATION = PostgresHint("PRESORT_AGG", 17, "enable_presorted_aggregate", True)

# partitions
PARTITION_WISE_AGGREGATE = PostgresHint("PART_AGG", 18, "enable_partitionwise_aggregate", False)
PARTITION_WISE_JOIN = PostgresHint("PART_JOIN", 19, "enable_partitionwise_join", False)
PARTITION_PRUNING = PostgresHint("PART_PRUNING", 20, "enable_partition_pruning", True)

# multi backend
ASYNC_APPEND = PostgresHint("ASYNC_APPEND", 16, "enable_async_append", True)

# GEQO
GEQO = PostgresHint("GEQO", 21, "geqo", True)

CORE_HINT_TUPLES = [
    INDEX_ONLY_SCAN,
    SEQ_SCAN,
    INDEX_SCAN,
    NESTED_LOOP_JOIN,
    MERGE_JOIN,
    HASH_JOIN
]

PG12_HINT_TUPLES = [
    *CORE_HINT_TUPLES,
    TID_SCAN,
    SORT,
    PARALLEL_HASH,
    PARALLEL_APPEND,
    MATERIALIZATION,
    HASH_AGGREGATION,
    GATHER_MERGE,
    BITMAP_SCAN
]

# Do not forget to reindex if the order of hint indexes is not increasingly built for custom builts.
PG13_HINT_TUPLES = reindex([*PG12_HINT_TUPLES, INCREMENTAL_SORT])
PG14_HINT_TUPLES = reindex([*PG13_HINT_TUPLES, MEMOIZE])
PG15_HINT_TUPLES = reindex([*PG14_HINT_TUPLES])
PG16_HINT_TUPLES = reindex([*PG15_HINT_TUPLES, PRESORTED_AGGREGATION])
PARTITION_HINT_TUPLES = [PARTITION_WISE_AGGREGATE, PARTITION_WISE_JOIN, PARTITION_PRUNING]
BACKEND_HINT_TUPLES = [ASYNC_APPEND]
MISC_HINT_TUPLES = [GEQO]

CORE_HINT_LIBRARY = HintLibrary([Hint(*entry) for entry in CORE_HINT_TUPLES])
PG_12_LIBRARY = HintLibrary([Hint(*entry) for entry in PG12_HINT_TUPLES])
PG_13_LIBRARY = HintLibrary([Hint(*entry) for entry in PG13_HINT_TUPLES])
PG_14_LIBRARY = HintLibrary([Hint(*entry) for entry in PG14_HINT_TUPLES])
PG_15_LIBRARY = HintLibrary([Hint(*entry) for entry in PG15_HINT_TUPLES])
PG_16_LIBRARY = HintLibrary([Hint(*entry) for entry in PG16_HINT_TUPLES])


def get_available_library(postgres_version: str,
                          use_partition_hints: bool = False,
                          use_misc: bool = True,
                          use_backend: bool = False) -> HintLibrary:
    hints = []
    pg_v = None
    if "12" in postgres_version:
        hints = PG12_HINT_TUPLES
        pg_v = 12
    if "13" in postgres_version:
        hints = PG13_HINT_TUPLES
        pg_v = 13
    if "14" in postgres_version:
        hints = PG14_HINT_TUPLES
        pg_v = 14
    if "15" in postgres_version:
        hints = PG15_HINT_TUPLES
        pg_v = 15
    if "16" in postgres_version:
        hints = PG16_HINT_TUPLES
        pg_v = 16
    if not hints or pg_v is None:
        raise ValueError(f"Unknown PostgreSQL version {postgres_version}")

    if use_partition_hints:
        hints.extend(PARTITION_HINT_TUPLES)
    if use_misc:
        hints.extend(MISC_HINT_TUPLES)
    if use_backend and pg_v > 13:
        hints.extend(BACKEND_HINT_TUPLES)

    hints = reindex(hints)
    return HintLibrary([Hint(*entry) for entry in hints])


def get_default_library() -> HintLibrary:
    return CORE_HINT_LIBRARY
