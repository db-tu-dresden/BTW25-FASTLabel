
from typing import Any
from fastgres.baseline.log_utils import get_logger


def get_select(parsed: Any):
    logger = get_logger()
    try:
        select_part = parsed['select']
    except:
        logger.info("Extraction of -select- unsuccessful, defaulting to -select_distinct-")
        try:
            select_part = parsed['select_distinct']
        except:
            logger.info(f"Extraction of -select_distinct- unsuccessful on {parsed}")
            raise ValueError("Failed to handle unknown select statement")
    return select_part

