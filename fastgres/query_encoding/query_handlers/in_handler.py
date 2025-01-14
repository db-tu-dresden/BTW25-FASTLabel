
from fastgres.baseline.log_utils import get_logger


def get_in_information(entry):
    key = 'in'
    in_statement = entry[key]
    try:
        alias, column = in_statement[0].split('.')
        value = in_statement[1]['literal']
    except (TypeError, ValueError):
        logger = get_logger()
        logger.info(f"Unhandled in-statement in {in_statement}")
        alias, column, key, value = [None] * 4
    return alias, column, key, value
