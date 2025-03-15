from typing import Optional, List, Dict, Any
import io
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from app.config_settings import settings
from logger_config import logger



# azure_connection_string = settings.AZURE_STORAGE_CONNECTION_STRING
# azure_container_name = settings.AZURE_STORAGE_CONTAINER_NAME

# def check_adls_connection(connection_string, container_name=None):
#     """
#     Tests connection to Azure Data Lake Storage and returns detailed results
#     """
#     connection_info = {}

#     # Try connecting with the connection string
#     try:
#         blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#         connection_info["connection_valid"] = True
#     except Exception as e:
#         connection_info["connection_valid"] = False
#         connection_info["connection_error"] = str(e)
#         return {
#             "success": False,
#             "timestamp": datetime.now().isoformat(),
#             "connection_details": connection_info
#         }
        
#     # Test container access if requested
#     if container_name:
#         try:
#             # Get the container client
#             container_client = blob_service_client.get_container_client(container_name)
            
#             # Test existence
#             exists = container_client.exists()
#             connection_info["container_exists"] = exists
            
#             if exists:
#                 # Get properties
#                 properties = container_client.get_container_properties()
#                 connection_info["container_properties"] = {
#                     "last_modified": properties.last_modified.isoformat() if hasattr(properties, "last_modified") else None,
#                     "lease_status": properties.lease.status if hasattr(properties, "lease") else None
#                 }
                
#                 # List a few blobs
#                 blobs = list(container_client.list_blobs(maxresults=3))
#                 connection_info["sample_blobs"] = [{"name": blob.name, "size": blob.size} for blob in blobs]
#                 connection_info["total_blobs_sampled"] = len(blobs)
#         except Exception as e:
#             connection_info["container_access_error"] = str(e)
#             connection_info["container_accessible"] = False
#             return {
#                 "success": False,
#                 "timestamp": datetime.now().isoformat(),
#                 "connection_details": connection_info
#             }
#         else:
#             connection_info["container_accessible"] = True
    
#     return {
#         "success": True,
#         "timestamp": datetime.now().isoformat(),
#         "connection_details": connection_info
#     }



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

    def upload_dataframe(self, df: pd.DataFrame, blob_name: str) -> bool:
        """
        Upload a DataFrame as a CSV to Azure Blob Storage
        
        Args:
            df: DataFrame to upload
            blob_name: Name of the blob to create
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure container exists
            self.ensure_container_exists()
            
            # Convert DataFrame to CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # Upload to blob storage
            blob_client = self.get_blob_client(blob_name)
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
            return True

        except Exception as e:
            logger.error(f"Error saving data to Azure: {str(e)}")
            return False


    async def check_connection(self) -> Dict[str, Any]:
        """
        Tests connection to Azure Data Lake Storage and returns detailed results
        
        Returns:
            Dictionary with connection test results
        """
        connection_info = {}

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
        except Exception as e:
            connection_info["container_access_error"] = str(e)
            connection_info["container_accessible"] = False
            return {
                "success": False,
                "timestamp": datetime.now().isoformat(),
                "connection_details": connection_info
            }
        else:
            connection_info["container_accessible"] = True
        
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "connection_details": connection_info
        }