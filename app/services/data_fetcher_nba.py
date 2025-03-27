import json
import pandas as pd
import time
from typing import List, Dict, Optional, Any

from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import teamdetails, playercareerstats
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


def parse_response(response: Dict[str, Any]) -> Dict[str, Any]:
    parsed_data = {}

    for result_set in response.get("resultSets", []):
        name = result_set["name"]  # Name of the result set (e.g., "TeamBackground")
        headers = result_set["headers"]  # Column headers
        rows = result_set["rowSet"]  # List of row values

        # If there's only one row, map headers to values as key-value pairs
        if len(rows) == 1:
            parsed_data[name] = dict(zip(headers, rows[0]))
        else:
            # Otherwise, create a list of dictionaries for multiple rows
            parsed_data[name] = [dict(zip(headers, row)) for row in rows]

    return parsed_data


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
        return players.get_active_players()
    except Exception as e:
        logger.error(f"Error getting players: {str(e)}")
        raise


def get_team_info_by_id(team_id: int) -> Dict:
    """Get detailed information about a specific NBA team."""
    try:
        team_details = teamdetails.TeamDetails(team_id=team_id).get_dict()
        parsed_team_details = parse_response(team_details)
        
        return parsed_team_details
    except Exception as e:
        logger.error(f"Error getting team info for ID {team_id}: {str(e)}")
        raise


def get_all_teams_info() -> List[Dict]:
    """Get detailed information about all NBA teams."""
    try:
        teams_data = get_all_teams()
        team_infos = []

        for team in teams_data:
            team_id = team.get('id')
            if team_id:
                logger.info(f"Getting team info for team_id: {team_id}")
                time.sleep(2) # Delay to avoid rate limit
                team_info = get_team_info_by_id(team_id=team_id)
                team_infos.append(team_info)

        return team_infos
    except Exception as e:
        logger.error(f"Error getting all team info: {str(e)}")
        raise


def get_player_career_stats_by_id(player_id: int) -> Dict:
    """Get detailed information about a specific NBA team."""
    try:
        career_stats = playercareerstats.PlayerCareerStats(player_id=player_id).get_dict()
        parsed_career_stats = parse_response(career_stats)
        
        return parsed_career_stats
    except Exception as e:
        logger.error(f"Error getting career stats for ID {player_id}: {str(e)}")
        raise


def get_all_players_career_stats() -> List[Dict]:
    """Get detailed information about all NBA teams."""
    try:
        players_data = get_all_players()
        all_player_career_stats = []

        for player in players_data:
            player_id = player.get('id')
            if player_id:
                logger.info(f"Getting career stats for player_id: {player_id}")
                time.sleep(2) # Delay to avoid rate limit
                player_stats = get_player_career_stats_by_id(player_id=player_id)
                all_player_career_stats.append(player_stats)

        return all_player_career_stats
    except Exception as e:
        logger.error(f"Error getting all active player career stats info: {str(e)}")
        raise