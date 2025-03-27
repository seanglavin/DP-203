import logging
import sys
import json
from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel


# Set up logger
logger = logging.getLogger("app")
logging.basicConfig(level=logging.INFO)

formatter = logging.Formatter(
    'LOGGER: [%(levelname)s] [%(asctime)s] | %(name)s.%(module)s.%(funcName)s - %(message)s'
    )

# Create file handler for logging to a file
file_handler = logging.FileHandler('logfile.log')
file_handler.setFormatter(formatter)

# Console handler for logging to stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

# Add both handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


# --- Log API Responses

class APILogEntry(BaseModel):
    """Standardized log format for API requests & responses."""
    response: Dict[str, Any]
    success: bool
    timestamp: datetime

    @classmethod
    def from_response(cls, response_model: BaseModel):
        """Creates a log entry from a response model."""
        return cls(
            success=response_model.success,
            timestamp=datetime.now(),
            response=response_model.dict(),
        )


def logger_api_response(
    response_model: BaseModel, 
    log_level="info"):
    """
    Extracts request parameters and logs API request/response in a standardized format.

    Args:
        response_model (BaseModel): The Pydantic response model.
        log_level (str): Log level ('info', 'warning', 'error'). Default is 'info'.
    """

    # Create log entry
    log_entry = APILogEntry.from_response(response_model)
    log_message = json.dumps(log_entry.dict(), indent=2, default=str)  # Convert datetime to string

    # Log at the appropriate level
    if log_level == "info":
        logger.info(log_message)
    elif log_level == "warning":
        logger.warning(log_message)
    elif log_level == "error":
        logger.error(log_message)
    else:
        logger.debug(log_message)