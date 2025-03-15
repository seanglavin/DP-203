from typing import List, Dict, Optional, Any, Union
from pydantic import BaseModel, Field, validator
from datetime import datetime

# --- Base Models ---

class StorageResponseBase(BaseModel):
    """Base model for all storage responses"""
    success: bool
    message: str


# --- File Models ---

class BlobFileInfo(BaseModel):
    """Information about a file in Azure Blob Storage"""
    name: str
    size_bytes: int
    created_on: Optional[str] = None
    last_modified: Optional[str] = None
    content_type: Optional[str] = None


class FileListResponse(StorageResponseBase):
    """Response model for listing files"""
    files: List[BlobFileInfo]
    count: int
    container: str


class FileDeleteResponse(StorageResponseBase):
    """Response model for file deletion"""
    filename: str


# --- Upload/Download Models ---

class FileUploadResponse(StorageResponseBase):
    """Response model for file uploads"""
    filename: str
    size_bytes: int
    rows: int


class DataFrameUploadRequest(BaseModel):
    """Request model for uploading a DataFrame"""
    filename: str
    overwrite: bool = False
    add_timestamp: bool = True


class DataFrameUploadResponse(StorageResponseBase):
    """Response model for DataFrame uploads"""
    filename: str
    rows: int


# --- Data Models ---

class DataDownloadResponse(StorageResponseBase):
    """Response model for data downloads in JSON format"""
    filename: str
    rows: int
    data: List[Dict[str, Any]]


# --- League Data Models ---

class LeagueSaveRequest(BaseModel):
    """Request model for saving league data"""
    league: str = Field(..., description="League to save (e.g., 'nba', 'nhl')")
    include_timestamp: bool = True
    
    @validator('league')
    def validate_league(cls, v):
        if v.lower() not in ['nba', 'nhl']:
            raise ValueError('League must be either "nba" or "nhl"')
        return v.lower()


class LeagueSaveResponse(StorageResponseBase):
    """Response model for saving league data"""
    league: str
    filename: str
    rows: int


# --- Connection Models ---

class StorageConnectionDetails(BaseModel):
    """Details about Azure Storage connection"""
    connection_valid: bool
    container_exists: Optional[bool] = None
    container_accessible: Optional[bool] = None
    container_properties: Optional[Dict[str, Any]] = None
    sample_blobs: Optional[List[Dict[str, Any]]] = None
    total_blobs_sampled: Optional[int] = None
    connection_error: Optional[str] = None
    container_access_error: Optional[str] = None


class ConnectionTestResponse(StorageResponseBase):
    """Response model for connection tests"""
    timestamp: str
    connection_details: StorageConnectionDetails