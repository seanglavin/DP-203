import json
from fastapi import HTTPException, APIRouter, Depends
from typing import List, Dict

from app.services.azure_storage_client import AzureDataStorageClient, get_storage_client
from app.services.data_fetcher_nba import (
    get_all_teams, 
    get_all_players, 
    convert_data_to_format,
    get_team_info_by_id,
    get_all_teams_info,
    get_all_players_career_stats
)
from app.config_settings import settings
from logger_config import logger



router = APIRouter()

 

# Use dependency injection for the storage client
async def get_storage():
    """Dependency to get storage client"""
    return get_storage_client()

# Root
@router.get("/", tags=["source"])
async def source_root():
    logger.info("message: NBA data fetching API root")
    return {"message": "NBA data fetching API root"}


# --- NBA Endpoints


@router.get("/nba/teams", response_model=List[Dict])
async def fetch_teams():
    """Get all NBA teams in specified format."""
    try:
        data = get_all_teams()
        data_json = convert_data_to_format(data, format_type="json")
        data_json_response = json.loads(data_json) if isinstance(data_json, str) else data_json
        
        # Limit log output to first 2 teams for readability
        sample_data = data_json_response[:2]
        logger.info(
            f"Successfully fetched {len(data_json_response)} teams. \nSample of first 2:\n{json.dumps(sample_data, indent=2)}"
        )
        return data
    except Exception as e:
        logger.error(f"Error processing team request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/nba/players", response_model=List[Dict])
async def fetch_players():
    """Get all NBA players in specified format."""
    try:
        data = get_all_players()
        data_json = convert_data_to_format(data, format_type="json")
        data_json_response = json.loads(data_json) if isinstance(data_json, str) else data_json

        # Limit log output to first 2 teams for readability
        sample_data = data_json_response[:2]
        logger.info(
            f"Successfully fetched {len(data_json_response)} players. \nSample of first 2:\n{json.dumps(sample_data, indent=2)}\n..."
        )
        return data
    except Exception as e:
        logger.error(f"Error processing player request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/nba/team_info/{team_id}", response_model=Dict)
async def fetch_team_info(team_id: int):
    """Get detailed information about a specific NBA team."""
    try:
        data = get_team_info_by_id(team_id=team_id)

        # Convert data to JSON string (limit length for logging)
        data_sample = json.dumps(data, indent=2)[:500]  # Log first 500 chars for readability

        logger.info(f"Successfully fetched info for team ID: {team_id}, \nSample response: {data_sample}\n...")
        return data
    except Exception as e:
        logger.error(f"Error processing team info request for ID {team_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/nba/team_info", response_model=List[Dict])
async def fetch_all_teams_info():
    """Get detailed information about all NBA teams."""
    try:
        data = get_all_teams_info()

        # Convert data to JSON string (limit length for logging)
        data_sample = json.dumps(data, indent=2)[:500]  # Log first 500 chars for readability

        logger.info(f"Successfully fetched info for {len(data)} teams, \nSample response: {data_sample}\n...")
        return data
    except Exception as e:
        logger.error(f"Error processing all team info request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    

@router.post("/nba/team-info/upload-csv")
async def upload_teams_to_csv(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """Upload all NBA teams data to Azure Storage as CSV."""
    try:
        # Fetch and convert team data to CSV format
        data = get_all_teams_info()

        # Upload to storage
        file_name = "nba_teams_info.csv"
        success = await storage_client.upload_data_as_csv(data=data, file_name=file_name)

        if success:
            logger.info(f"Successfully uploaded {len(data)} teams' info to Azure Storage")
            return {"message": f"Uploaded {len(data)} teams' info to Azure Storage"}
        else:
            raise HTTPException(status_code=500, detail="Failed to upload team info data")

    except Exception as e:
        logger.error(f"Error uploading team info data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/nba/players/upload-csv")
async def upload_players_to_csv(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """Upload all NBA players data to Azure Storage as CSV."""
    try:
        # Fetch and convert player data to CSV format
        data = get_all_players()

        # Upload to storage
        file_name = "nba_players.csv"
        success = await storage_client.upload_data_as_csv(data=data, file_name=file_name)

        if success:
            logger.info(f"Successfully uploaded {len(data)} players to Azure Storage")
            return {"message": f"Uploaded {len(data)} players to Azure Storage"}
        else:
            raise HTTPException(status_code=500, detail="Failed to upload players data")

    except Exception as e:
        logger.error(f"Error uploading player data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    

@router.post("/nba/players/career-stats/upload-csv")
async def upload_all_players_career_stats_to_csv(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """Upload all NBA players career stats data to Azure Storage as CSV."""
    try:
        # Fetch and convert player data to CSV format
        data = get_all_players_career_stats()

        # Upload to storage
        file_name = "nba_players_careerstats.csv"
        success = await storage_client.upload_data_as_csv(data=data, file_name=file_name)

        if success:
            logger.info(f"Successfully uploaded {len(data)} player career stats to Azure Storage")
            return {"message": f"Uploaded {len(data)} player career stats to Azure Storage"}
        else:
            raise HTTPException(status_code=500, detail="Failed to upload player career stats")

    except Exception as e:
        logger.error(f"Error uploading player career stats data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))