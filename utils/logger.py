import logging
import logging.handlers
import sys
from typing import Dict, Any

def setup_logger(config:Dict[str,Any])->logging.Logger:
    """
    Set up a configured logger
    Args:
        config: Logging configuration
    Returns:
        configured logger
    """
    #get configuration
    log_level=getattr(logging, config["level"].upper())
    log_format=config["format"]
    log_file = config["file"]
    max_bytes = config["max_size"]
    backup_count = config["backup_count"]

    #create logger
    logger = logging.getLogger("etl_pipeline")
    logger.setLevel(log_level)
    
    #Remove existing handler
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    #create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(log_format)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    #create file handler
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger
