from typing import List, Dict, Optional, Any, Union
from pydantic import BaseModel, Field, validator
from datetime import datetime



# --- Connection Models ---

class StorageConnectionDetails(BaseModel):
    """Details about Azure Storage connection."""
    connection_valid: bool
    container_exists: Optional[bool] = None
    container_accessible: Optional[bool] = None
    container_properties: Optional[Dict[str, Any]] = None
    sample_blobs: Optional[List[Dict[str, Any]]] = None
    total_blobs_sampled: Optional[int] = None
    connection_error: Optional[str] = None
    container_access_error: Optional[str] = None


class ConnectionTestResponse(BaseModel):
    """Response model for connection tests."""
    success: bool
    message: str
    timestamp: datetime
    connection_details: StorageConnectionDetails

    @validator('timestamp', pre=True, always=True)
    def parse_timestamp(cls, v):
        """Parse timestamp string into a datetime object."""
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v)
            except ValueError:
                raise ValueError("Timestamp string must be in ISO 8601 format")
        return v


# --- Azure models

class FileListResponse(BaseModel):
    """Response model for listing files."""
    name: str
    size: int
    last_modified: Optional[datetime] = None

class DataDownloadResponse(BaseModel):
    """Response model for downloaded data."""
    data: Union[List[Dict], Dict, str]
    file_name: str
    download_time: str

class DataFrameUploadRequest(BaseModel):
    """Request model for uploading transformed data."""
    file_name: str
    data: Union[List[Dict], Dict]

class DataFrameUploadResponse(BaseModel):
    """Response model for successful data upload."""
    message: str


# --- Reads single parquet file
class ParquetReadResponse(BaseModel):
    """Response model for reading parquet files."""
    message: str
    file_name: str
    execution_time: str
    sample_data: Optional[List[Dict]] = None
    data: List[Dict]
    
    class Config:
        arbitrary_types_allowed = True

# ---

class QueryResultsResponse(BaseModel):
    """Response model for query results."""
    data: List[Dict]
    query_params: Dict
    result_count: int
    execution_time: str

class DataQueryRequest(BaseModel):
    """Request model for querying data."""
    file_name: str
    filter_params: Optional[Dict[str, Any]] = None


# ---

class ParquetDataDownloadResponse(BaseModel):
    """Response model for downloading parquet files."""
    file_name: str
    data: List[Dict]
    download_time: str

class QueryResultsResponse(BaseModel):
    """Response model for SQL query results."""
    results: List[Dict]
    columns: List[str]
    row_count: int
    execution_time: str



class ParquetReadDataResponse(BaseModel):
    success: bool
    count: int
    sample_data: Optional[List[Dict]] = None
    data: List[Dict]

    class Config:
        arbitrary_types_allowed = True

class ParquetMergeResponse(BaseModel):
    success: bool
    total_records: int
    sample_data: List[Dict]
    merged_file: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class ParquetFilterResponse(BaseModel):
    success: bool
    total_records: int
    filtered_record_counts: Dict[str, int]

    class Config:
        arbitrary_types_allowed = True

