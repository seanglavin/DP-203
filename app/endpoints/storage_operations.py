import json
import io
import pandas as pd
from fastapi import HTTPException, APIRouter, Depends
from typing import List, Dict, Optional
from datetime import datetime

from app.services.azure_storage_client import AzureDataStorageClient, get_storage_client
from app.models.azure_storage import FileListResponse, DataDownloadResponse, DataFrameUploadRequest, DataFrameUploadResponse
from logger_config import logger

router = APIRouter()

# Use dependency injection for the storage client
async def get_storage():
    """Dependency to get storage client"""
    return get_storage_client()


@router.get("/storage/files", response_model=List[FileListResponse])
async def list_files(prefix: Optional[str] = None, max_results: int = 100, storage_client: AzureDataStorageClient = Depends(get_storage)):
    """List files in the Azure Storage container."""
    try:
        files = await storage_client.list_files(prefix=prefix, max_results=max_results)
        return files
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/storage/file/{file_name}", response_model=DataDownloadResponse)
async def download_file(file_name: str, storage_client: AzureDataStorageClient = Depends(get_storage)):
    """Download a specific file from Azure Storage."""
    try:
        blob_client = storage_client.get_blob_client(blob_name=file_name)
        blob_data = await blob_client.download_blob()

        # Return the content as JSON or text
        if file_name.endswith('.csv'):
            df = pd.read_csv(io.StringIO(blob_data.content))
            return {"data": df.to_dict('records'), "file_name": file_name, "download_time": datetime.now().isoformat()}
        elif file_name.endswith('.json'):
            data = json.loads(blob_data.content)
            return {"data": data, "file_name": file_name, "download_time": datetime.now().isoformat()}
        else:
            return {"content": blob_data.content.decode('utf-8'), "file_name": file_name, "download_time": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Error downloading file {file_name}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    

# @router.post("/storage/transform/csv-to-parquet")
# async def transform_csv_to_parquet(request: DataFrameUploadRequest, storage_client: AzureDataStorageClient = Depends(get_storage)):
#     """Transform CSV data to Parquet format and upload to storage."""
#     try:
#         # Download the CSV file
#         blob_data = await storage_client.download_blob(file_name=request.file_name)

#         # Convert CSV to DataFrame
#         df = pd.read_csv(io.StringIO(blob_data))

#         # Apply ETL transformations
#         transformed_df = apply_etl_transformations(df)

#         # Upload transformed data as Parquet
#         parquet_file_name = f"transformed_{request.file_name.replace('.csv', '.parquet')}"
#         success = await storage_client.upload_dataframe(
#             df=transformed_df,
#             blob_name=parquet_file_name
#         )

#         if success:
#             return {"message": f"Successfully transformed and uploaded {parquet_file_name}"}
#         else:
#             raise HTTPException(status_code=500, detail="Failed to upload transformed data")

#     except Exception as e:
#         logger.error(f"Error transforming CSV file {request.file_name}: {str(e)}", exc_info=True)
#         raise HTTPException(status_code=500, detail=str(e))