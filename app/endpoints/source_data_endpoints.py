import json
import pandas as pd
from fastapi import HTTPException, APIRouter, Depends
from typing import List, Dict

from app.services.azure_storage_client import AzureDataStorageClient, get_storage_client
from app.services.data_fetcher_nba import (
    get_all_teams, 
    get_all_active_players,
    get_team_info_by_id,
    get_all_teams_info,
    get_all_active_players_career_stats
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


@router.get("/teams", response_model=List[Dict])
async def fetch_teams():
    """
    Get all NBA teams.
    """
    try:
        df = get_all_teams()
        data_json_response = df.to_dict(orient="records")
        
        logger.info(f"Successfully fetched {len(data_json_response)} teams. Sample: {json.dumps(data_json_response[:2], indent=2)}")
        return data_json_response
    except Exception as e:
        logger.error(f"Error processing team request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/teams")
async def upload_teams_list(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Upload NBA teams data for 10 previous seasons to Azure Storage as parquet.
    
    """
    try:
        df = get_all_teams()
        file_name = "nba_teams.parquet"

        success = await storage_client.upload_data_as_parquet(data=df, file_name=file_name)

        if success:
            logger.info(f"Successfully uploaded {len(df)} teams to Azure Storage")
            return {"message": f"Uploaded {len(df)} teams to Azure Storage"}
        else:
            raise HTTPException(status_code=500, detail="Failed to upload teams data")
    except Exception as e:
        logger.error(f"Error processing team request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/players", response_model=List[Dict])
async def fetch_active_players():
    """
    Get all NBA players.
    """
    try:
        df = get_all_active_players()
        data_json_response = df.to_dict(orient="records")

        logger.info(f"Successfully fetched {len(data_json_response)} players. Sample: {json.dumps(data_json_response[:2], indent=2)}")
        return data_json_response
    except Exception as e:
        logger.error(f"Error processing player request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/teams-info")
async def upload_teams_info(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Upload NBA teams data for 10 previous seasons to Azure Storage as parquet.
    
    """
    try:
        teams_info_dict = get_all_teams_info()

        # # Create a timestamp for versioning
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Upload each data set as a separate file
        results = {}
        for dataset_name, df in teams_info_dict.items():
            if not df.empty:
                file_name = f"nba_teams_info/{dataset_name}.parquet"
                
                # Upload file
                success = await storage_client.upload_data_as_parquet(data=df, file_name=file_name)
                
                if success:
                    logger.info(f"Successfully uploaded {dataset_name} data with {len(df)} rows to Azure Storage")
                    results[dataset_name] = {
                        "rows": len(df),
                        "success": True,
                        "files": [file_name]
                    }
                else:
                    results[dataset_name] = {
                        "rows": len(df),
                        "success": False
                    }
        
        return {
            "message": f"Uploaded team info as Parquet files for 10 previous seasons",
            "dataset_results": results
        }
    except Exception as e:
        logger.error(f"Error uploading team info data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/players")
async def upload_active_players_list(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Upload all NBA players data to Azure Storage as parquet.
    """
    try:
        df = get_all_active_players()
        file_name = "nba_players.parquet"

        success = await storage_client.upload_data_as_parquet(data=df, file_name=file_name)

        if success:
            logger.info(f"Successfully uploaded {len(df)} players to Azure Storage")
            return {"message": f"Uploaded {len(df)} players to Azure Storage"}
        else:
            raise HTTPException(status_code=500, detail="Failed to upload players data")

    except Exception as e:
        logger.error(f"Error uploading player data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    

@router.post("/players/career-stats")
async def upload_all_active_players_career_stats(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Upload all NBA players' career stats data to Azure Storage as partitioned Parquet files.
    """
    try:
        df = get_all_active_players_career_stats()
        partition_column = "SEASON_ID"  # Change this based on your data
        partitions = df[partition_column].unique()

        # Upload each partition separately
        for partition in partitions:
            partition_df = df[df[partition_column] == partition]
            file_name = f"nba_players_careerstats/season={partition}.parquet"

            success = await storage_client.upload_data_as_parquet(data=partition_df, file_name=file_name)

            if success:
                logger.info(f"Successfully uploaded data for season {partition} to Azure Storage")
        
        return {"message": f"Uploaded player career stats as partitioned Parquet files"}

    except Exception as e:
        logger.error(f"Error uploading player career stats data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
