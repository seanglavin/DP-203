import io
import pandas as pd
from typing import List, Dict
import requests
from datetime import datetime
from azure.storage.blob import BlobServiceClient

from logger_config import logger
from app.config_settings import settings
from app.services.data_storage import AzureDataStorageClient



def extract_all_nba_teams() -> pd.DataFrame:
    """
    Extracts data for all NBA teams using the TheSportsDB API.
    
    Returns:
        DataFrame containing all NBA team information
    """
    api_base = settings.THESPORTSDB_FREE_API_BASE
    logger.info("Extracting all NBA teams...")
    
    try:

        # Try fetching data from TheSportsDB API endpoint
        logger.info("Fetching NBA team data from API")
        try:
            # TheSportsDB API endpoint to search teams by league
            response = requests.get(
                f"{api_base}/search_all_teams.php?l=NBA",
                timeout=15
            )
            
            if not response.ok:
                logger.error(f"API request failed for NBA teams. Status code: {response.status_code}")
                return pd.DataFrame()
                
            data = response.json()
            all_teams = []
            
            if "teams" in data and data["teams"]:
                for team_info in data["teams"]:
                    all_teams.append({
                        "idTeam": team_info.get("idTeam"),
                        "strTeam": team_info.get("strTeam"),
                        "strTeamShort": team_info.get("strTeamShort"),
                        "intFormedYear": team_info.get("intFormedYear"),
                        "strSport": team_info.get("strSport"),
                        "strLeague": team_info.get("strLeague"),
                        "idLeague": team_info.get("idLeague"),
                        "strStadium": team_info.get("strStadium"),
                        "strLocation": team_info.get("strLocation"),
                        "intStadiumCapacity": team_info.get("intStadiumCapacity"),
                        "strLogo": team_info.get("strLogo"),
                        "timestamp": datetime.now().isoformat()
                    })
                    
            return pd.DataFrame(all_teams)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for NBA teams: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Error extracting NBA team data: {str(e)}")
        return None
    

def extract_all_nhl_teams() -> pd.DataFrame:
    """
    Extracts data for all NHL teams using the TheSportsDB API.
    
    Returns:
        DataFrame containing all NHL team information
    """
    api_base = settings.THESPORTSDB_FREE_API_BASE
    logger.info("Extracting all NHL teams...")
    

    try:
        try:
            # TheSportsDB API endpoint to search teams by league
            response = requests.get(
                f"{api_base}/search_all_teams.php?l=NHL",
                timeout=15
            )
            
            if not response.ok:
                logger.error(f"API request failed for NHL teams. Status code: {response.status_code}")
                return pd.DataFrame()
                
            data = response.json()
            all_teams = []
            
            if "teams" in data and data["teams"]:
                for team_info in data["teams"]:
                    all_teams.append({
                        "idTeam": team_info.get("idTeam"),
                        "strTeam": team_info.get("strTeam"),
                        "strTeamShort": team_info.get("strTeamShort"),
                        "intFormedYear": team_info.get("intFormedYear"),
                        "strSport": team_info.get("strSport"),
                        "strLeague": team_info.get("strLeague"),
                        "idLeague": team_info.get("idLeague"),
                        "strStadium": team_info.get("strStadium"),
                        "strLocation": team_info.get("strLocation"),
                        "intStadiumCapacity": team_info.get("intStadiumCapacity"),
                        "strLogo": team_info.get("strLogo"),
                        "timestamp": datetime.now().isoformat()
                    })
                    
            return pd.DataFrame(all_teams)

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for NHL teams: {str(e)}")
            return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Error extracting NHL team data: {str(e)}")
        return None



def extract_team_players(team_id: int) -> pd.DataFrame:
    """
    Extracts all players for a specific team using TheSportsDB API.

    Args:
        team_id: ID of the team

    Returns:
        DataFrame containing player information, or empty DataFrame if not found
    """
    api_base = settings.THESPORTSDB_FREE_API_BASE
    logger.info(f"Extracting players for team ID: {team_id}")

    try:
        response = requests.get(
            f"{api_base}/lookup_all_players.php?id={team_id}",
            timeout=15
        )

        if not response.ok:
            logger.error(f"API request failed for team players. Status code: {response.status_code}")
            return pd.DataFrame()

        data = response.json()

        if "players" in data and data["players"]:
            players = []

            for player_info in data["players"]:
                players.append({
                    "idPlayer": player_info.get("idPlayer"),
                    "idTeam": player_info.get("idTeam"),
                    "strNationality": player_info.get("strNationality"),
                    "strPlayer": player_info.get("strPlayer"),
                    "strTeam": player_info.get("strTeam"),
                    "strSport": player_info.get("strSport"),
                    "dateBorn": player_info.get("dateBorn"),
                    "strNumber": player_info.get("strNumber"),
                    "strWage": player_info.get("strWage"),
                    "strBirthLocation": player_info.get("strBirthLocation"),
                    "strStatus": player_info.get("strStatus"),
                    "strHeight": player_info.get("strHeight"),
                    "strWeight": player_info.get("strWeight"),
                    "strThumb": player_info.get("strThumb"),
                    "timestamp": datetime.now().isoformat()
                })

            return pd.DataFrame(players)

        return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for team players: {str(e)}")
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error extracting team players: {str(e)}")
        return None

def extract_team_details(team_id: int) -> pd.DataFrame:
    """
    Extracts detailed information about a specific team using TheSportsDB API.

    Args:
        team_id: ID of the team

    Returns:
        DataFrame containing team details, or empty DataFrame if not found
    """
    api_base = settings.THESPORTSDB_FREE_API_BASE
    logger.info(f"Extracting details for team ID: {team_id}")

    try:
        response = requests.get(
            f"{api_base}/lookupteam.php?id={team_id}",
            timeout=15
        )

        if not response.ok:
            logger.error(f"API request failed for team details. Status code: {response.status_code}")
            return pd.DataFrame()

        data = response.json()

        if "teams" in data and data["teams"]:
            teams = []

            for team_info in data["teams"]:
                teams.append({
                    "idTeam": team_info.get("idTeam"),
                    "strTeam": team_info.get("strTeam"),
                    "strTeamShort": team_info.get("strTeamShort"),
                    "intFormedYear": team_info.get("intFormedYear"),
                    "strSport": team_info.get("strSport"),
                    "strLeague": team_info.get("strLeague"),
                    "idLeague": team_info.get("idLeague"),
                    "strStadium": team_info.get("strStadium"),
                    "strLocation": team_info.get("strLocation"),
                    "intStadiumCapacity": team_info.get("intStadiumCapacity"),
                    "strLogo": team_info.get("strLogo"),
                    "timestamp": datetime.now().isoformat()
                })

            return pd.DataFrame(teams)

        return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for team details: {str(e)}")
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error extracting team details: {str(e)}")
        return None

def extract_player_details(player_id: int) -> pd.DataFrame:
    """
    Extracts detailed information about a specific player using TheSportsDB API.

    Args:
        player_id: ID of the player

    Returns:
        DataFrame containing player details, or empty DataFrame if not found
    """
    api_base = settings.THESPORTSDB_FREE_API_BASE
    logger.info(f"Extracting details for player ID: {player_id}")

    try:
        response = requests.get(
            f"{api_base}/lookupplayer.php?id={player_id}",
            timeout=15
        )

        if not response.ok:
            logger.error(f"API request failed for player details. Status code: {response.status_code}")
            return pd.DataFrame()

        data = response.json()

        if "players" in data and data["players"]:
            players = []

            for player_info in data["players"]:
                players.append({
                    "idPlayer": player_info.get("idPlayer"),
                    "idTeam": player_info.get("idTeam"),
                    "strNationality": player_info.get("strNationality"),
                    "strPlayer": player_info.get("strPlayer"),
                    "strTeam": player_info.get("strTeam"),
                    "strSport": player_info.get("strSport"),
                    "dateBorn": player_info.get("dateBorn"),
                    "strNumber": player_info.get("strNumber"),
                    "strWage": player_info.get("strWage"),
                    "strBirthLocation": player_info.get("strBirthLocation"),
                    "strStatus": player_info.get("strStatus"),
                    "strHeight": player_info.get("strHeight"),
                    "strWeight": player_info.get("strWeight"),
                    "strThumb": player_info.get("strThumb"),
                    "timestamp": datetime.now().isoformat()
                })

            return pd.DataFrame(players)

        return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for player details: {str(e)}")
        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error extracting player details: {str(e)}")
        return None