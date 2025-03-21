from fastapi import APIRouter, HTTPException, Query, BackgroundTasks, File, UploadFile, Depends
from fastapi.responses import StreamingResponse
from typing import List, Optional
import pandas as pd
import io
from datetime import datetime

from app.models.azure_storage import (
    FileListResponse, FileDeleteResponse, FileUploadResponse, 
    DataDownloadResponse, LeagueSaveRequest, LeagueSaveResponse
)
from app.services.get_storage_client import get_storage_client
from app.services.data_fetcher import extract_all_nba_teams, extract_all_nhl_teams
from app.config_settings import settings
from logger_config import logger, logger_api_response


router = APIRouter()

# Use dependency injection for the storage client
async def get_storage():
    """Dependency to get storage client"""
    return get_storage_client()


@router.get("/", tags=["storage"])
async def storage_root():
    return {"message": "Azure Storage API"}


@router.post("/save/{league}", response_model=LeagueSaveResponse, tags=["storage"])
async def save_league_data(league: str, background_tasks: BackgroundTasks, data_storage = Depends(get_storage)):
    """
    Fetches and saves data for a specific sports league to Azure Blob Storage
    
    Parameters:
    - league: League to fetch data for (e.g., "nba", "nhl")
    """
    try:
        logger.info(f"Saving {league.upper()} data to Azure storage")
        
        # Validate league parameter
        if league.lower() not in ["nba", "nhl"]:
            raise HTTPException(status_code=400, detail=f"Unsupported league: {league}")
        
        # Fetch data based on the league
        if league.lower() == "nba":
            df = extract_all_nba_teams()
        elif league.lower() == "nhl":
            df = extract_all_nhl_teams()
        
        if df is None or df.empty:
            return LeagueSaveResponse(
                success=False,
                message=f"No {league.upper()} data found to save",
                league=league.lower(),
                filename="",
                rows=0
            )
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d")
        filename = f"{league.lower()}_teams_{timestamp}.csv"
        
        # Save data in background task to avoid blocking
        background_tasks.add_task(
            data_storage.upload_dataframe,
            dataframe=df,
            file_name=filename
        )
        
        return LeagueSaveResponse(
            success=True,
            message=f"Saving {league.upper()} data to '{filename}' in progress",
            league=league.lower(),
            rows=len(df),
            filename=filename
        )
    
    except Exception as e:
        logger.error(f"Error saving {league} data to Azure: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error saving data: {str(e)}")


@router.get("/files", response_model=FileListResponse, tags=["storage"])
async def list_files(prefix: Optional[str] = None, max_results: int = Query(100, ge=1, le=5000), data_storage = Depends(get_storage)):
    """
    Lists files in Azure Blob Storage
    
    Parameters:
    - prefix: Optional prefix to filter files
    - max_results: Maximum number of results to return (1-5000)
    """
    try:
        files = []
        blobs = data_storage.container_client.list_blobs(name_starts_with=prefix, max_results=max_results)
        
        for blob in blobs:
            files.append({
                "name": blob.name,
                "size_bytes": blob.size,
                "created_on": blob.creation_time.isoformat() if blob.creation_time else None,
                "last_modified": blob.last_modified.isoformat() if blob.last_modified else None,
                "content_type": blob.content_settings.content_type if blob.content_settings else None
            })
        
        return FileListResponse(
            success=True,
            message=f"Successfully listed {len(files)} files",
            files=files,
            count=len(files),
            container=settings.AZURE_STORAGE_CONTAINER_NAME
        )
    
    except Exception as e:
        logger.error(f"Error listing files in Azure storage: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")


# @router.get("/download/{filename}", tags=["storage"])
# async def download_file(filename: str, format: str = "json", data_storage = Depends(get_storage)):
#     """
#     Downloads a file from Azure Blob Storage
    
#     Parameters:
#     - filename: Name of the file to download
#     - format: Response format (json or csv)
#     """
#     try:
#         logger.info(f"Downloading file '{filename}' from Azure storage")
        
#         if format.lower() not in ["json", "csv"]:
#             raise HTTPException(status_code=400, detail=f"Unsupported format: {format}")
        
#         # Get the blob client
#         blob_client = data_storage.get_blob_client(filename)

#         # Check if blob exists
#         try:
#             blob_properties = blob_client.get_blob_properties()
#         except Exception as e:
#             raise HTTPException(status_code=404, detail=f"File '{filename}' not found")

#         # Download the blob content
#         download_stream = blob_client.download_blob()
#         content = download_stream.readall()

#         # Convert to DataFrame
#         try:
#             df = pd.read_csv(io.BytesIO(content))
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Error parsing CSV data: {str(e)}")
        
#         # Return based on requested format
#         if format.lower() == "json":
#             records = df.to_dict('records')
#             return DataDownloadResponse(
#                 success=True,
#                 message=f"Successfully downloaded '{filename}'",
#                 filename=filename,
#                 rows=len(df),
#                 data=records
#             )
#         else:  # CSV format

#             # Return as streaming response
#             return StreamingResponse(
#                 io.BytesIO(content),
#                 media_type="text/csv",
#                 headers={"Content-Disposition": f"attachment; filename={filename}"}
#             )
    
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error downloading file '{filename}': {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error downloading file: {str(e)}")


@router.delete("/files/{filename}", response_model=FileDeleteResponse, tags=["storage"])
async def delete_file(filename: str, data_storage = Depends(get_storage)):
    """
    Deletes a file from Azure Blob Storage
    
    Parameters:
    - filename: Name of the file to delete
    """
    try:
        logger.info(f"Deleting file '{filename}' from Azure storage")
        
        blob_client = data_storage.get_blob_client(filename)
        
        # Check if the blob exists
        try:
            blob_client.get_blob_properties()
        except Exception:
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found")
        
        # Delete the blob
        blob_client.delete_blob()
        
        return FileDeleteResponse(
            success=True,
            message=f"File '{filename}' deleted successfully",
            filename=filename
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting file '{filename}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error deleting file: {str(e)}")

 

# --- Convenience endpoint

@router.post("/extract-and-save/{league}", response_model=LeagueSaveResponse, tags=["storage"])
async def extract_and_save_league_data(league: str, background_tasks: BackgroundTasks, data_storage = Depends(get_storage)):
    """
    Combined endpoint that extracts sports data and saves it to Azure storage in one operation
    
    Parameters:
    - league: League to fetch and save data for (e.g., "nba", "nhl")
    """
    try:
        logger.info(f"Extracting and saving {league.upper()} data to Azure storage")
        
        # Validate league parameter
        if league.lower() not in ["nba", "nhl"]:
            raise HTTPException(status_code=400, detail=f"Unsupported league: {league}")
        
        # Fetch data based on the league
        if league.lower() == "nba":
            df = extract_all_nba_teams()
        elif league.lower() == "nhl":
            df = extract_all_nhl_teams()
        
        if df is None or df.empty:
            response = LeagueSaveResponse(
                success=False,
                message=f"No {league.upper()} data found to save",
                league=league.lower(),
                filename="",
                rows=0
            )
            logger_api_response(response)
            return response
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d")
        filename = f"{league.lower()}_teams_{timestamp}.csv"
        
        # Save data in background task to avoid blocking
        background_tasks.add_task(
            data_storage.upload_dataframe,
            dataframe=df,
            file_name=filename
        )
        
        response = LeagueSaveResponse(
            success=True,
            message=f"Extracted and saved {league.upper()} data to '{filename}' in progress",
            league=league.lower(),
            rows=len(df),
            filename=filename
        )
        logger_api_response(response)
        return response
    
    except Exception as e:
        logger.error(f"Error in extract and save operation for {league}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error in extract and save operation: {str(e)}")