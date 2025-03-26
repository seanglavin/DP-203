import json
import pandas as pd
from typing import List, Dict, Optional
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import teaminfocommon,  commonplayerinfo

from logger_config import logger
from app.config_settings import settings


# --- Helpers

def convert_data_to_format(data: Dict, format_type: str):
    """
    Convert data to specified format.

    Args:
        data (Dict): The raw data to convert
        format_type (str): One of 'json', 'dict', 'csv',
                          'normalized_json', 'normalized_dict'
    Returns:
        Converted data or None if format not supported
    """
    if format_type == 'json':
        return json.dumps(data)
    elif format_type == 'dict':
        return data
    elif format_type == 'csv':
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
    elif format_type in ['normalized_json', 'normalized_dict']:
        # Normalize nested structures if needed
        normalized_data = []
        for item in data:
            normalized_item = {}
            for key, value in item.items():
                if isinstance(value, dict):
                    # Flatten nested dictionaries
                    for k, v in value.items():
                        normalized_item[f"{key}_{k}"] = v
                else:
                    normalized_item[key] = value
            normalized_data.append(normalized_item)
        if format_type == 'normalized_json':
            return json.dumps(normalized_data)
        else:
            return normalized_data
    else:
        logger.error(f"Unsupported format: {format_type}")
        return None


# --- NBA_API functions

def get_all_teams() -> List[Dict]:
    """Get all NBA teams and their details."""
    try:
        return teams.get_teams()
    except Exception as e:
        logger.error(f"Error getting teams: {str(e)}")
        raise

def get_all_players() -> List[Dict]:
    """Get all NBA players and their details."""
    try:
        return players.get_players()
    except Exception as e:
        logger.error(f"Error getting players: {str(e)}")
        raise
