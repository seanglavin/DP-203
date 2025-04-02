from typing import Optional, List, Dict, Any
import io
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from app.config_settings import settings
from logger_config import logger
import json


# --- Helpers
def get_storage_client():
    """
    Factory function that returns storage client

    """
    # Return client
    return AzureDataStorageClient(
        connection_string=settings.AZURE_STORAGE_CONNECTION_STRING,
        container_name=settings.AZURE_STORAGE_CONTAINER_NAME
    )


# --- Class
class AzureDataStorageClient:
    """
    Azure Data Lake Storage client for managing data operations
    """

    def __init__(self, connection_string: str, container_name: str):
        """
        Initialize the DataStorage client
        
        Args:
            connection_string: Azure Storage connection string
            container_name: Container name
        """
        self.connection_string = connection_string
        self.container_name = container_name
        self._blob_service_client = None
        self._container_client = None

    @property
    def blob_service_client(self) -> BlobServiceClient:
        """
        Get or create BlobServiceClient instance
        """
        if not self._blob_service_client:
            self._blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        return self._blob_service_client

    @property
    def container_client(self) -> ContainerClient:
        """
        Get or create ContainerClient instance
        """
        if not self._container_client:
            self._container_client = self.blob_service_client.get_container_client(self.container_name)
        return self._container_client


    def get_blob_client(self, blob_name: str) -> BlobClient:
        """Get a BlobClient for a specific blob
        
        Args:
            blob_name: Name of the blob
        
        Returns:
            BlobClient instance
        """
        return self.blob_service_client.get_blob_client(
            container=self.container_name,
            blob=blob_name
        )
    
    async def ensure_container_exists(self) -> bool:
        """
        Create the container if it doesn't exist
        
        Returns:
            True if container exists or was created, False on error
        """
        try:
            if not self.container_client.exists():
                self.blob_service_client.create_container(self.container_name)
                logger.info(f"Created container: {self.container_name}")
            return True
        except Exception as e:
            logger.error(f"Error ensuring container exists: {str(e)}")
            return False

    async def list_files(self, prefix: Optional[str] = None, max_results: int = 100) -> List[Dict[str, Any]]:
        """
        List files in the container
        
        Args:
            prefix: Optional filename prefix filter
            max_results: Maximum number of results to return
            
        Returns:
            List of file metadata dictionaries
        """
        try:
            # Ensure container exists
            await self.ensure_container_exists()
            
            # List blobs with optional prefix and max results
            blobs = self.container_client.list_blobs(
                name_starts_with=prefix, 
                maxresults=max_results
            )
            
            # Convert blob properties to list of dictionaries
            file_list = [
                {
                    "name": blob.name,
                    "size": blob.size,
                    "last_modified": blob.last_modified
                } 
                for blob in blobs
            ]
            
            return file_list
    
        except Exception as e:
            logger.error(f"Error listing files in container: {str(e)}")
            raise


# ---

    async def upload_data_as_parquet(self, data: pd.DataFrame, file_name: str) -> bool:
        """
        Upload data as a Parquet file to Azure Storage.

        Args:
            data: Pandas DataFrame to store
            file_name: Path in Azure Blob Storage

        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure container exists
            await self.ensure_container_exists()

            # Convert DataFrame to Parquet bytes
            parquet_buffer = io.BytesIO()
            data.to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy")
            parquet_buffer.seek(0)

            # Upload to storage
            blob_client = self.get_blob_client(file_name)
            blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)

            return True

        except Exception as e:
            logger.error(f"Error uploading Parquet data: {str(e)}")
            raise

    async def upload_data_as_csv(self, data: pd.DataFrame, file_name: str) -> bool:
        """
        Upload NBA team info data in specified format to Azure Storage.

        Args:
            data: List of team info dictionaries

        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure container exists
            await self.ensure_container_exists()

            csv_buffer = io.StringIO()
            data.to_csv(csv_buffer, index=False)
            blob_name = file_name

            # Upload to storage
            blob_client = self.get_blob_client(blob_name)
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

            return True

        except Exception as e:
            logger.error(f"Error uploading team info data: {str(e)}")
            raise


    async def upload_data_as_json(self, data: pd.DataFrame, file_name: str) -> bool:
        """
        Upload NBA team info data in specified format to Azure Storage.

        Args:
            data: List of team info dictionaries

        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure container exists
            await self.ensure_container_exists()

            data_dict = data.to_dict(index=False)
            json_data = json.dumps(data_dict)
            blob_name = file_name

            # Upload to storage
            blob_client = self.get_blob_client(blob_name)
            blob_client.upload_blob(json_data, overwrite=True)

            return True

        except Exception as e:
            logger.error(f"Error uploading team info data: {str(e)}")
            raise


    async def read_parquet_data(self, file_name: str) -> pd.DataFrame:
        """
        Read a Parquet file from Azure Storage.
        
        Args:
            file_name: Path to the Parquet file in Azure Blob Storage
            
        Returns:
            Pandas DataFrame containing the data, or None if the file doesn't exist
        """
        try:
            # Ensure container exists
            await self.ensure_container_exists()

            # Get blob client
            blob_client = self.get_blob_client(file_name)
            
            # Check if blob exists
            if not blob_client.exists():
                logger.warning(f"Parquet file {file_name} does not exist")
                return None
            
            # Download blob content
            blob_data = blob_client.download_blob().readall()

            # logging block
            if not blob_data:
                logger.info(f"Read blob parquet data failed for: {file_name}")
            logger.info(f"Read blob parquet data success for: {file_name}")
            
            # Convert to DataFrame
            buffer = io.BytesIO(blob_data)
            df = pd.read_parquet(buffer, engine="pyarrow")

            return df
        except Exception as e:
            logger.error(f"Error reading Parquet data from {file_name}: {str(e)}")
            raise































    async def check_connection(self) -> Dict[str, Any]:
        """
        Tests connection to Azure Data Lake Storage and returns detailed results
        
        Returns:
            Dictionary with connection test results
        """
        connection_info = {
            "connection_valid": False,
            "container_exists": None,
            "container_accessible": None,
            "container_properties": None,
            "sample_blobs": None,
            "total_blobs_sampled": None,
            "connection_error": None,
            "container_access_error": None
        }

        # Try connecting with the connection string
        try:
            # Use the existing blob_service_client property
            _ = self.blob_service_client
            connection_info["connection_valid"] = True
        except Exception as e:
            connection_info["connection_valid"] = False
            connection_info["connection_error"] = str(e)
            return {
                "success": False,
                "timestamp": datetime.now().isoformat(),
                "connection_details": connection_info
            }
            
        # Test container access
        try:
            # Use the container_client property
            exists = self.container_client.exists()
            connection_info["container_exists"] = exists
            
            if exists:
                # Get properties
                properties = self.container_client.get_container_properties()
                connection_info["container_properties"] = {
                    "last_modified": properties.last_modified.isoformat() if hasattr(properties, "last_modified") else None,
                    "lease_status": properties.lease.status if hasattr(properties, "lease") else None
                }
                
                # List a few blobs
                blobs = list(self.container_client.list_blobs(maxresults=3))
                connection_info["sample_blobs"] = [{"name": blob.name, "size": blob.size} for blob in blobs]
                connection_info["total_blobs_sampled"] = len(blobs)
                connection_info["container_accessible"] = True

        except Exception as e:
            connection_info["container_access_error"] = str(e)
            connection_info["container_accessible"] = False
            return {
                "success": False,
                "timestamp": datetime.now().isoformat(),
                "connection_details": connection_info
            }
          
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "connection_details": connection_info
        }