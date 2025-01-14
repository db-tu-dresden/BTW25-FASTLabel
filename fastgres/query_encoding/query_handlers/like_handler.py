
from fastgres.baseline.log_utils import get_logger


def get_like_information(entry):
    key = 'like'
    like_statement = entry[key]
    try:
        alias, column = like_statement[0].split('.')
        value = like_statement[1]['literal']
    except:
        logger = get_logger()
        logger.info(f"Extracting like information through lower prefix on {like_statement}")
        try:
            alias, column = like_statement[0]['lower'].split('.')
            value = like_statement[1]['lower']['literal']
        except:
            logger.info(f"Extracting like information not possible in {like_statement}")
            alias, column, key, value = [None] * 4
    return alias, column, key, value
