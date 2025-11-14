"""Logging configuration for SOAR pipeline.

Provides structured logging with configurable levels, JSON formatting for production,
and human-readable formatting for development.
"""

import logging
import logging.config
from pathlib import Path
from typing import Optional

# Default logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detailed": {
            "format": "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "json": {
            "format": "%(asctime)s %(name)s %(levelname)s %(lineno)d %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%SZ",
            "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
        },
        "simple": {
            "format": "%(levelname)s: %(message)s",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "detailed",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "filename": "logs/soar_pipeline.log",
            "maxBytes": 10 * 1024 * 1024,  # 10MB
            "backupCount": 5,
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"],
    },
    "loggers": {
        "soar": {
            "level": "DEBUG",
            "handlers": ["console", "file"],
            "propagate": False,
        },
        "aqs": {
            "level": "DEBUG",
            "handlers": ["console", "file"],
            "propagate": False,
        },
    },
}


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    json_format: bool = False,
    verbose: bool = False,
) -> None:
    """Configure logging for the application.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        json_format: Use JSON formatting for structured logging
        verbose: Enable verbose output (DEBUG level)
    """
    # Apply configuration parameters to logging config
    config = LOGGING_CONFIG.copy()

    # Set log level
    if verbose:
        level = "DEBUG"

    numeric_level = getattr(logging, level.upper(), logging.INFO)
    config["root"]["level"] = level

    # Set console formatter
    if json_format:
        config["handlers"]["console"]["formatter"] = "json"
    else:
        config["handlers"]["console"]["formatter"] = "detailed"

    # Configure file handler if log_file specified
    if log_file:
        config["handlers"]["file"]["filename"] = log_file
        # Ensure logs directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        # Disable file handler if no log file specified
        config["handlers"].pop("file", None)
        # Remove file handler from logger configurations
        for logger_config in config["loggers"].values():
            if "file" in logger_config["handlers"]:
                logger_config["handlers"].remove("file")

    # Apply configuration
    logging.config.dictConfig(config)

    # Log the configuration
    logger = logging.getLogger(__name__)
    logger.info(
        f"Logging configured: level={level}, json_format={json_format}, log_file={log_file}"
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


# Convenience functions for common logging patterns
def log_pipeline_start(pipeline_name: str, **context) -> None:
    """Log the start of a pipeline execution."""
    logger = get_logger("soar.pipeline")
    logger.info(f"Starting pipeline: {pipeline_name}", extra=context)


def log_pipeline_end(pipeline_name: str, success: bool = True, **context) -> None:
    """Log the end of a pipeline execution."""
    logger = get_logger("soar.pipeline")
    status = "completed successfully" if success else "failed"
    logger.info(f"Pipeline {pipeline_name} {status}", extra=context)


def log_api_call(endpoint: str, method: str = "GET", **context) -> None:
    """Log API call details."""
    logger = get_logger("aqs.api")
    logger.debug(f"API call: {method} {endpoint}", extra=context)


def log_data_processing(operation: str, record_count: int, **context) -> None:
    """Log data processing operations."""
    logger = get_logger("soar.processing")
    logger.info(f"Data processing: {operation} - {record_count} records", extra=context)


def log_error_with_context(error: Exception, operation: str, **context) -> None:
    """Log errors with additional context."""
    logger = get_logger("soar.errors")
    logger.error(f"Error in {operation}: {error}", extra=context, exc_info=True)
