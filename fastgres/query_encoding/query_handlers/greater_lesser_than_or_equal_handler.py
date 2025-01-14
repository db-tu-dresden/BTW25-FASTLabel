
from fastgres.baseline.log_utils import get_logger


def get_gtlte_information(entry, entry_keys):
    key = 'gte' if 'gte' in entry_keys else 'lte'
    glte_statement = entry[key]
    try:
        alias, column = glte_statement[0].split('.')
        value = glte_statement[1]
    except ValueError:
        # no alias was used, currently unhandled
        # alias = None
        # column = glte_statement[0]
        logger = get_logger()
        logger.info(f"Unhandled greater-lesser-than-equal case in {glte_statement}")
        alias, column, key, value = [None] * 4
    return alias, column, key, value
