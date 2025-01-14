
from fastgres.baseline.log_utils import get_logger


def get_not_like_information(entry):
    key = 'not_like'
    not_like_statement = entry[key]
    try:
        alias, column = not_like_statement[0].split('.')
        value = not_like_statement[1]['literal']
    except (TypeError, ValueError):
        logger = get_logger()
        logger.info(f"Did not recognize not_like statement in {not_like_statement}")
        alias, column, key, value = [None] * 4
    return alias, column, key, value
