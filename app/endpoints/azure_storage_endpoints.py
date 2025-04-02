import json
import io
import pandas as pd
from fastapi import HTTPException, APIRouter, Depends
from typing import List, Dict, Optional, Any
from datetime import datetime

from app.services.azure_storage_client import AzureDataStorageClient, get_storage_client
from app.services.data_transformations import process_player_career_stats_nested_parquet_data
from app.models.azure_storage_models import FileListResponse, ParquetReadResponse
from logger_config import logger

router = APIRouter()

# Use dependency injection for the storage client
async def get_storage():
    """Dependency to get storage client"""
    return get_storage_client()



# --- Listing files
 

@router.get("/files", response_model=List[FileListResponse])
async def list_parquet_files(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    List all Parquet files in the Azure Storage container.
    """
    logger.info("Received GET request | list_parquet_files")
    try:
        # Filter for .parquet files
        files = await storage_client.list_files()
        parquet_files = [f for f in files if f["name"].endswith(".parquet")]

        logger.info(f"Found {len(parquet_files)} Parquet files")
        return parquet_files

    except Exception as e:
        logger.error(f"Error listing Parquet files: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



# --- Reading files


@router.get("/players")
async def read_players_list(
    # team_id: Optional[int] = None,
    # season_id: Optional[str] = None,
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Retrieve NBA active players list.
    
    Returns:
        The data as JSON
    """
    logger.info("Received GET request | read_players_list")
    try:

        # Define the file path
        file_name = "nba_players.parquet"
        
        # Read the parquet file
        data = await storage_client.read_parquet_data(file_name)
        
        if data is None or data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset {file_name} not found or empty"
            )
        
        # Convert to dict for JSON response
        result = data.to_dict(orient="records")
        return {"data": result, "count": len(result)}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving {file_name} data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve data: {str(e)}")
    

@router.get("/teams")
async def read_teams_list(
    # team_id: Optional[int] = None,
    # season_id: Optional[str] = None,
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Retrieve NBA teams list.
    
    Returns:
        The data as JSON
    """
    try:
        logger.info("Received GET request | read_teams_list")
        # Define the file path
        file_name = "nba_teams.parquet"
        
        # Read the parquet file
        data = await storage_client.read_parquet_data(file_name)
        
        if data is None or data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset {file_name} not found or empty"
            )
        
        # Convert to dict for JSON response
        result = data.to_dict(orient="records")
        return {"data": result, "count": len(result)}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving {file_name} data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve data: {str(e)}")


@router.get("/team_info/TeamInfoCommon")
async def read_team_info_common(
    # team_id: Optional[int] = None,
    # season_id: Optional[str] = None,
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Retrieve NBA teams' common info data from Azure Storage Parquet files.
    
    Returns:
        The data as JSON
    """
    try:
        logger.info("Received GET request | read_team_info_common")
        # Define the file path
        file_name = "nba_teams_info/TeamInfoCommon.parquet"
        
        # Read the parquet file
        data = await storage_client.read_parquet_data(file_name)
        
        if data is None or data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset {file_name} not found or empty"
            )
        
        # Convert to dict for JSON response
        result = data.to_dict(orient="records")
        return {"data": result, "count": len(result)}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving {file_name} data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve data: {str(e)}")
    

@router.get("/team_info/TeamSeasonRanks")
async def read_team_season_ranks(
    # team_id: Optional[int] = None,
    # season_id: Optional[str] = None,
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Retrieve NBA teams' season ranks data from Azure Storage Parquet files.
    
    Returns:
        The data as JSON
    """
    try:
        logger.info("Received GET request | read_team_season_ranks")
        # Define the file path
        file_name = "nba_teams_info/TeamSeasonRanks.parquet"
        
        # Read the parquet file
        data = await storage_client.read_parquet_data(file_name)
        
        if data is None or data.empty:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset {file_name} not found or empty"
            )
        
        # Convert to dict for JSON response
        result = data.to_dict(orient="records")
        return {"data": result, "count": len(result)}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving {file_name} data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve data: {str(e)}")
    




@router.get("/parquet/{file_name}", response_model=ParquetReadResponse)
async def read_parquet_file(file_name: str, storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Download a Parquet file and return its contents as JSON.
    """
    logger.info(f"Received GET request | read_parquet_file - {file_name}")
    try:
        df = await storage_client.read_parquet_data(file_name)

        if df is None:
            raise HTTPException(status_code=404, detail=f"Parquet file '{file_name}' not found")

        # Convert DataFrame to JSON-compatible format
        processed_data = df.to_dict("records")  # Convert DataFrame to list of dicts

        # # Process nested data structure
        # processed_data = process_player_career_stats_nested_parquet_data(df)

        # Convert DataFrame to list of dictionaries
        data_records = df.to_dict('records')

        # Get schema information
        schema_info = {
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
        }

        logger.info(f"Successfully read {file_name} with {len(processed_data)} records")
        return {
            "message": f"Successfully downloaded {file_name}",
            "file_name": file_name,
            "execution_time": datetime.now().isoformat(),
            "sample_data": processed_data[:3],
            "data": processed_data
        }
    
    except HTTPException:
        raise  # Re-raise HTTPException to maintain HTTP status codes
    except Exception as e:
        logger.error(f"Error processing Parquet file {file_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e), headers={"X-Debug-Info": f"File not found: {file_name}"})

