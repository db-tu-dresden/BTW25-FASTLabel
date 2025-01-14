
from fastgres.baseline.log_utils import get_logger


def get_neq_information(entry):
    key = 'neq'
    neq_statement = entry[key]
    try:
        alias, column = neq_statement[0].split('.')
        value = neq_statement[1]['literal']
    except (TypeError, ValueError):
        logger = get_logger()
        logger.info(f"Not able to encode neq information in {neq_statement}")
        alias, column, key, value = [None] * 4
    return alias, column, key, value
