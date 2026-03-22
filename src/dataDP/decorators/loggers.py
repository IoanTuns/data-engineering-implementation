"""Module for logging decorators to enhance visibility and debugging in Databricks jobs."""

import functools
import time
from typing import Any, Callable, TypeVar

from dataDP.spark_context import get_spark_session
from dataDP.utils.logger import logger

# Generic type for function return values
R = TypeVar("R")


def with_logging(func: Callable[..., R]) -> Callable[..., R]:
    """
    A decorator that logs the start, end, and duration of a function execution.

    This decorator provides visibility into task execution within Databricks jobs,
    capturing any exceptions and logging the total time taken for the operation.

    Args:
        func (callable): The function to be wrapped and monitored.

    Returns:
        callable: The wrapped function (wrapper) that includes logging logic.

    Raises:
        Exception: Re-raises any exception encountered during the execution of
            the wrapped function after logging the error.
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> R:
        start_time = time.time()
        logger.info(f"==> Starting Task: {func.__name__}")

        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"==> Completed {func.__name__} in {duration:.2f}s")
            return result
        except Exception as e:
            # Captures the full stack trace for easier debugging in Databricks logs
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            raise e

    return wrapper


def with_logging_and_spark(func: Callable[..., R]) -> Callable[..., R]:
    """A decorator that injects a SparkSession into the wrapped function.

    In a Databricks environment, this will retrieve the existing active
    session. In a local environment, it will create a new Spark session
    based on the available configuration. This is particularly useful
    for modularizing code within Databricks Asset Bundles.

    Args:
        func (callable): The function to be wrapped. The first argument
            of this function must be a SparkSession object.

    Returns:
        callable: The wrapped function that automatically receives
            the 'spark' session as its first argument.
    """

    @functools.wraps(func)
    @with_logging
    def wrapper(*args: Any, **kwargs: Any) -> R:
        spark = get_spark_session()

        return func(spark, *args, **kwargs)

    return wrapper
