
from fastgres.baseline.log_utils import get_logger


def get_table_entries(parsed_from) -> dict:
    table_dict = dict()
    logger = get_logger()
    for entry in parsed_from:
        try:
            # alias - name
            table_dict[entry['name']] = entry['value']
        except (KeyError, TypeError):
            try:
                table_dict[entry] = entry
            except:
                logger.info(f"Unknown table result in {parsed_from}")
                raise ValueError("Unknown table structure")
    return table_dict
