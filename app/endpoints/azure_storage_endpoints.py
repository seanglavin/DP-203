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


# Get list of all active players
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
    

# Get list of all teams
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


# Get all teams common info
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
    

# Get all teams season ranks
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
    

# Get full team info
@router.get("/team_info/full")
async def read_full_team_info(
    team_id: Optional[int] = None,
    season_year: Optional[str] = None,
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Retrieve merged NBA team info and season ranks data from Azure Storage Parquet files.
    Allows filtering by Team ID and Season ID.
    """
    try:
        logger.info("Received GET request | read_full_team_info")
        
        # Define file paths
        team_info_file = "nba_teams_info/TeamInfoCommon.parquet"
        team_ranks_file = "nba_teams_info/TeamSeasonRanks.parquet"
        
        # Read parquet files
        team_info_df = await storage_client.read_parquet_data(team_info_file)
        team_ranks_df = await storage_client.read_parquet_data(team_ranks_file)
        
        if team_info_df is None or team_info_df.empty or team_ranks_df is None or team_ranks_df.empty:
            raise HTTPException(status_code=404, detail="One or both datasets not found or empty")
        
        # Transform SEASON_YEAR to match SEASON_ID format
        team_info_df["SEASON_ID"] = "2" + team_info_df["SEASON_YEAR"].str[:4]
        
        # Merge dataframes on TEAM_ID and SEASON_ID
        merged_df = pd.merge(team_info_df, team_ranks_df, on=["TEAM_ID", "SEASON_ID"], how="inner")
        
        # Apply filters if provided
        if team_id is not None:
            merged_df = merged_df[merged_df["TEAM_ID"] == team_id]
        if season_year is not None:
            season_id = "2" + season_year  # Convert to SEASON_ID format
            merged_df = merged_df[merged_df["SEASON_ID"] == season_id]
        
        if merged_df.empty:
            raise HTTPException(status_code=404, detail="No data found for given filters")
        
        # Convert to dict for JSON response
        result = merged_df.to_dict(orient="records")
        return {"data": result, "count": len(result)}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving merged team data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve data: {str(e)}")
    


### ---


# @router.get("/parquet/{file_name}", response_model=ParquetReadResponse)
# async def read_parquet_file(file_name: str, storage_client: AzureDataStorageClient = Depends(get_storage)):
#     """
#     Download a Parquet file and return its contents as JSON.
#     """
#     logger.info(f"Received GET request | read_parquet_file - {file_name}")
#     try:
#         df = await storage_client.read_parquet_data(file_name)

#         if df is None:
#             raise HTTPException(status_code=404, detail=f"Parquet file '{file_name}' not found")

#         # Convert DataFrame to JSON-compatible format
#         processed_data = df.to_dict("records")  # Convert DataFrame to list of dicts

#         # # Process nested data structure
#         # processed_data = process_player_career_stats_nested_parquet_data(df)

#         # Convert DataFrame to list of dictionaries
#         data_records = df.to_dict('records')

#         # Get schema information
#         schema_info = {
#             "columns": list(df.columns),
#             "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
#         }

#         logger.info(f"Successfully read {file_name} with {len(processed_data)} records")
#         return {
#             "message": f"Successfully downloaded {file_name}",
#             "file_name": file_name,
#             "execution_time": datetime.now().isoformat(),
#             "sample_data": processed_data[:3],
#             "data": processed_data
#         }
    
#     except HTTPException:
#         raise  # Re-raise HTTPException to maintain HTTP status codes
#     except Exception as e:
#         logger.error(f"Error processing Parquet file {file_name}: {str(e)}", exc_info=True)
#         raise HTTPException(status_code=500, detail=str(e), headers={"X-Debug-Info": f"File not found: {file_name}"})

