import io
import os
import pandas as pd
from typing import List, Dict
import requests
from datetime import datetime
from azure.storage.blob import BlobServiceClient

from logger_config import logger
from app.config_settings import settings
from app.services.data_storage import DataStorage



def extract_all_nba_teams() -> pd.DataFrame:
    """
    Extracts data for all NBA teams using the TheSportsDB API.
    
    Returns:
        DataFrame containing all NBA team information
    """
    api_base = settings.THESPORTSDB_FREE_API_BASE
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
    api_base = settings.THESPORTSDB_FREE_API_BASE
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
    

def save_data_to_azure(data: pd.DataFrame, file_name: str) -> bool:
    """
    Save DataFrame to Azure Data Lake Storage.

    Args:
        data: DataFrame to save
        file_name: Name of the file in storage

    Returns:
        True if successful, False otherwise
    """
    try:
        # Initialize blob service client
        blob_service_client = BlobServiceClient.from_connection_string(settings.AZURE_STORAGE_CONNECTION_STRING)
        container_name = settings.AZURE_STORAGE_CONTAINER_NAME

        # Convert DataFrame to CSV buffer
        csv_buffer = io.StringIO()
        data.to_csv(csv_buffer, index=False)

        # Upload to Azure Data Lake
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.upload_text(csv_buffer.getvalue())

        return True

    except Exception as e:
        logger.error(f"Error saving data to Azure: {str(e)}")
        return False