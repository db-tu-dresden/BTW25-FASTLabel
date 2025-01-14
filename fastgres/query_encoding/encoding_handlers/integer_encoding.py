
from fastgres.baseline.log_utils import get_logger
from collections.abc import Callable


def get_integer_encoding(mm_enc: Callable[[float, float, float, float], float],
                         filter_value: float, mm_d: dict, table: str, column: str):
    # some weird join in stack
    if isinstance(filter_value, str):
        logger = get_logger()
        logger.info("Probably some undetected joins: {} ".format(filter_value))
        return 0.0
    offset = 0.001
    # encode min max
    min_v, max_v = mm_d[table][column]
    return mm_enc(min_v, max_v, filter_value, offset)
