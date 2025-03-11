import os
import logging
import pandas as pd
from typing import List, Dict
import requests
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_team_data(team_names: List[str]) -> pd.DataFrame:
    """
    Extracts team data from TheSportsDB API for a list of team names.

    Args:
        team_names: List of team names to look up

    Returns:
        DataFrame containing extracted team information
    """
    api_base = "https://www.thesportsdb.com/api/v1/json/3"

    all_teams = []

    for team in team_names:
        logger.info(f"Processing team: {team}")

        try:
            response = requests.get(
                f"{api_base}/searchteams.php?t={team}",
                timeout=10
            )

            if not response.ok:
                logger.error(f"API request failed for {team}. Status code: {response.status_code}")
                continue

            data = response.json()

            if "teams" in data and len(data["teams"]) > 0:
                team_info = data["teams"][0]  # Take first matching team
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
                    "timestamp": datetime.now()
                })

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {team}: {str(e)}")

    return pd.DataFrame(all_teams)


def extract_all_nba_teams() -> pd.DataFrame:
    """
    Extracts data for all NBA teams using the TheSportsDB API.
    
    Returns:
        DataFrame containing all NBA team information
    """
    api_base = "https://www.thesportsdb.com/api/v1/json/3"
    logger.info("Extracting all NBA teams...")
    
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
                    "timestamp": datetime.now()
                })
                
        return pd.DataFrame(all_teams)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for NBA teams: {str(e)}")
        return pd.DataFrame()


def extract_all_nhl_teams() -> pd.DataFrame:
    """
    Extracts data for all NHL teams using the TheSportsDB API.
    
    Returns:
        DataFrame containing all NHL team information
    """
    api_base = "https://www.thesportsdb.com/api/v1/json/3"
    logger.info("Extracting all NHL teams...")
    
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
                    "timestamp": datetime.now()
                })
                
        return pd.DataFrame(all_teams)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error for NHL teams: {str(e)}")
        return pd.DataFrame()