"""Logger configuration for the DataDP project."""

import logging
import os


def setup_logger(name: str = "DataDP_Project", log_path: str | None = None) -> logging.Logger:
    """Sets up a logger that logs to both console and a file in Unity Catalog Volume.

    Args:
        name (str, optional): The name of the logger. Defaults to "DataDP_Project".
        log_path (str, optional): The path to the log file in Unity Catalog Volume. Defaults to None.

    Returns:
        logging.Logger: The configured logger.
    """
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(log_level)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # 1. Console Handler (pentru vizualizare în timp real)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # 2. File Handler (pentru audit în Unity Catalog Volume)
        # From Global
        log_destination = os.getenv("LOG_FILE_PATH")
        # From input param
        if log_path:
            path = log_path
        elif log_destination:
            path = log_destination
        else:
            path = None

        if path:
            # Chek if filepath exists and has access(pe Volumes funcționează ca un sistem de fișiere local)
            log_dir = os.path.dirname(path)
            if not os.path.exists(log_dir):
                msg = f"Log directory path {log_dir} does not exist."
                logger.error(msg)
                raise FileNotFoundError(msg)
            elif not os.access(log_dir, os.W_OK):
                msg = f"No write access to log directory path {log_dir}."
                logger.error(msg)
                raise PermissionError(msg)

            file_handler = logging.FileHandler(path)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.info(f"Log-urile vor fi salvate și în: {path}")

        logger.propagate = False

    return logger


logger = setup_logger()
