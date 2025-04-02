import json
import pandas as pd
import time
from datetime import datetime
from typing import List, Dict, Optional, Any

from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import teaminfocommon, playercareerstats
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


def get_all_teams() -> pd.DataFrame:
    """
    Get all NBA teams and their details.
    """
    try:
        teams_data = teams.get_teams()
        logger.info("")
        return pd.DataFrame(teams_data)
    except Exception as e:
        logger.error(f"Error getting teams: {str(e)}")
        raise


def get_all_active_players() -> pd.DataFrame:
    """
    Get all NBA players and their details.
    """
    try:
        players_data = players.get_active_players()
        return pd.DataFrame(players_data)
    except Exception as e:
        logger.error(f"Error getting players: {str(e)}")
        raise


def get_team_info_by_id(team_id: int) -> Dict:
    """
    Get detailed information about a specific NBA team.
    
    Returns a dictionary with all data frames from the endpoint.
    """
    try:
        team_details = teaminfocommon.TeamInfoCommon(team_id=team_id)
        data_frames = team_details.get_data_frames()
        
        # Create a dictionary to store all dataframes with their names
        result = {
            "TeamInfoCommon": data_frames[0],
            "TeamSeasonRanks": data_frames[1],
        }
                
        return result
    except Exception as e:
        logger.error(f"Error getting team info for ID {team_id}: {str(e)}")
        raise


def get_team_info_by_id_and_season(team_id: int, season: str) -> Dict:
    """
    Get detailed information about a specific NBA team for a specific season.
    
    Args:
        team_id: The NBA team ID
        season: The season in format "YYYY-YY" (e.g., "2023-24")
    
    Returns a dictionary with all data frames from the endpoint.
    """
    try:
        # Call the API with required parameters
        team_details = teaminfocommon.TeamInfoCommon(team_id=team_id, season_nullable=season)
        data_frames = team_details.get_data_frames()
        
        # Create a dictionary to store all dataframes with their names
        result = {
            "TeamInfoCommon": data_frames[0],
            "TeamSeasonRanks": data_frames[1],
        }
        
        return result
    except Exception as e:
        logger.error(f"Error getting team info for ID {team_id} season {season}: {str(e)}")
        raise

def get_ten_seasons():
    """
    Generate a list of the most recent NBA seasons in "YYYY-YY" format.
    
    Returns:
        List of season strings in format "YYYY-YY"
    """
    # Get current year
    num_seasons=10
    current_year = datetime.now().year
    
    # Determine most recent season based on current month
    # If we're before October, the most recent completed season ended in the current year
    # Otherwise, it will be next year's season
    if datetime.now().month < 10:
        most_recent_end_year = current_year
    else:
        most_recent_end_year = current_year + 1
    
    seasons = []
    for i in range(num_seasons):
        end_year = most_recent_end_year - i
        start_year = end_year - 1
        season = f"{start_year}-{str(end_year)[-2:]}"
        seasons.append(season)
    
    return seasons


def get_all_teams_info() -> Dict:
    """
    Get detailed information about all NBA teams for multiple seasons.
    
    Returns a dictionary with all teams' data organized by data set type.
    """
    try:
        teams_df = get_all_teams()
        seasons = get_ten_seasons()
        
        # Initialize empty lists for each data set
        all_team_info_common = []
        all_team_season_ranks = []
        
        total_requests = len(teams_df) * len(seasons)
        completed = 0
        
        for _, team in teams_df.iterrows():
            team_id = team["id"]
            if team_id:
                for season in seasons:
                    try:
                        time.sleep(1.2)  # Delay to avoid rate limit
                        team_info_dict = get_team_info_by_id_and_season(team_id=team_id, season=season)

                        if team_info_dict:
                            completed += 1
                            logger.info(f"Got team_info for team_id: {team_id}, season: {season} ({completed}/{total_requests})")


                            all_team_info_common.append(team_info_dict["TeamInfoCommon"])
                            all_team_season_ranks.append(team_info_dict["TeamSeasonRanks"])

                    except Exception as e:
                        logger.warning(f"Failed to get data for team_id: {team_id}, season: {season}: {str(e)}")
                        continue
        
        # Combine all data for each data set
        result = {
            "TeamInfoCommon": pd.concat(all_team_info_common, ignore_index=True) if all_team_info_common else pd.DataFrame(),
            "TeamSeasonRanks": pd.concat(all_team_season_ranks, ignore_index=True) if all_team_season_ranks else pd.DataFrame()
        }
        

        # # Drop duplicates if any
        # for key in result:
        #     if not result[key].empty and 'TEAM_ID' in result[key].columns and 'SEASON_ID' in result[key].columns:
        #         result[key] = result[key].drop_duplicates(subset=['TEAM_ID', 'SEASON_ID'])


        return result
    except Exception as e:
        logger.error(f"Error getting all team info: {str(e)}")
        raise


def get_player_career_stats_by_id(player_id: int) -> pd.DataFrame:
    """
    Get detailed information about a specific NBA team.
    """
    try:
        career_stats = playercareerstats.PlayerCareerStats(player_id=player_id)
        df = career_stats.get_data_frames()[0]  # Extract the first DataFrame from the response
        df["player_id"] = player_id  # Ensure player_id is in the dataset
        return df
    except Exception as e:
        logger.error(f"Error fetching career stats for player {player_id}: {str(e)}")
        return pd.DataFrame()


def get_all_active_players_career_stats() -> pd.DataFrame:
    """
    Get detailed information about all NBA teams.
    """
    try:
        players_df = get_all_active_players()
        all_player_career_stats = []

        for _, player in players_df.iterrows():
            player_id = player["id"]
            if player_id:
                time.sleep(1)  # Delay to avoid rate limits
                player_stats = get_player_career_stats_by_id(player_id=player_id)
                if not player_stats.empty:
                    logger.info(f"Got career stats for player_id: {player_id}")
                    all_player_career_stats.append(player_stats)

        # Combine all player stats into a single DataFrame
        full_df = pd.concat(all_player_career_stats, ignore_index=True) if all_player_career_stats else pd.DataFrame()

        if not full_df.empty:
            # Get the most recent 10 seasons
            most_recent_seasons = sorted(full_df['SEASON_ID'].unique(), reverse=True)[:10]
            full_df = full_df[full_df['SEASON_ID'].isin(most_recent_seasons)]

        return full_df
    except Exception as e:
        logger.error(f"Error getting all active player career stats: {str(e)}")
        raise