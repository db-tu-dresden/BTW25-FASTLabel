
import logging
import warnings
from typing import Optional

from fastgres.definitions import PathConfig


class Logger:

    def __init__(self, path_config: PathConfig, log_name: Optional[str]):
        if log_name is None:
            warnings.warn("Using default_log_name as no log name was provided.")
            log_name = "default_log_name.log"
        logging.basicConfig(filename=path_config.LOG_DIR + f'/{log_name}', filemode='w',
                            format='%(asctime)s %(name)s - %(levelname)s - %(message)s', level=logging.INFO)


def get_logger():
    """
    Interface to obtain a basic logger that suffices for our needs.
    :return: logger at name "Logger" with log level set at "INFO"
    """
    logger = logging.getLogger("Logger")
    logger.setLevel(logging.INFO)
    return logger
