
from fastgres.baseline.log_utils import get_logger


def get_gtlt_information(entry, entry_keys):
    key = 'gt' if 'gt' in entry_keys else 'lt'
    glt_statement = entry[key]
    try:
        alias, column = glt_statement[0].split('.')
        value = glt_statement[1]
        # string gt filters
        if isinstance(value, dict):
            try:
                value = value['literal']
            except:
                logger = get_logger()
                try:
                    # catching dates
                    value = value['cast'][0]['literal']
                except:
                    # gt join date + interval
                    logger.info(f"Encoding gt with date interval {glt_statement}")
                    alias, column, key, value = [None] * 4
    except:
        logger = get_logger()
        logger.info(f"Error encoding gt using {glt_statement}")
        alias, column, key, value = [None] * 4

    return alias, column, key, value
