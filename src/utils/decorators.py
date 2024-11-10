import logging
from functools import wraps
import pandas as pd


def log_execution_and_errors(logger_name, dataframe_attr=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            instance = args[0]
            class_name = instance.__class__.__name__
            logger = logging.getLogger(logger_name)
            logger.info(f"Starting {func.__name__} from {class_name}")
            try:
                result = func(*args, **kwargs)
                if dataframe_attr:
                    dataframe = getattr(instance, dataframe_attr, None)
                    if isinstance(dataframe, pd.DataFrame):
                        logger.info(
                            f"DataFrame ({dataframe_attr}) ending shape: {dataframe.shape}"
                        )
                logger.info(f"Finished {func.__name__} successfully")
                return result
            except Exception as e:
                logger.error(f"Error during {class_name}.{func.__name__}: {e}")
                raise
        return wrapper
    return decorator