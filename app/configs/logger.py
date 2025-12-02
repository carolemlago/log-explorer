"""Central logging configuration for the Log Explorer app."""

import logging
import sys

# Create logger
logger = logging.getLogger("log_explorer")
logger.setLevel(logging.DEBUG)

# Remove any existing handlers to avoid duplicates
if logger.hasHandlers():
    logger.handlers.clear()

# Create formatter
formatter = logging.Formatter(
    fmt='%(asctime)s.%(msecs)03d | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Console handler - outputs to stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(console_handler)

# Prevent propagation to root logger
logger.propagate = False


def get_logger(name: str) -> logging.Logger:
    """Get a child logger with the given name."""
    return logging.getLogger(f"log_explorer.{name}")

