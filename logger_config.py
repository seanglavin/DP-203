import logging
import sys
import json
import pandas as pd
from datetime import datetime
from typing import Any, Dict, List
from pydantic import BaseModel


# Set up logger
logger = logging.getLogger("app")
logging.basicConfig(level=logging.INFO)

formatter = logging.Formatter(
    '[%(levelname)s] [%(asctime)s] | %(name)s.%(module)s.%(funcName)s - %(message)s'
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
