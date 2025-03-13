from typing import Optional
import os
import io
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import pandas as pd
from app.config_settings import settings
from logger_config import logger



azure_connection_string = settings.AZURE_STORAGE_CONNECTION_STRING
azure_container_name = settings.AZURE_STORAGE_CONTAINER_NAME

def check_adls_connection(connection_string, container_name=None):
    """
    Tests connection to Azure Data Lake Storage and returns detailed results
    """
    connection_info = {}

    # Try connecting with the connection string
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        connection_info["connection_valid"] = True
    except Exception as e:
        connection_info["connection_valid"] = False
        connection_info["connection_error"] = str(e)
        return {
            "success": False,
            "timestamp": datetime.now().isoformat(),
            "connection_details": connection_info
        }
        
    # Test container access if requested
    if container_name:
        try:
            # Get the container client
            container_client = blob_service_client.get_container_client(container_name)
            
            # Test existence
            exists = container_client.exists()
            connection_info["container_exists"] = exists
            
            if exists:
                # Get properties
                properties = container_client.get_container_properties()
                connection_info["container_properties"] = {
                    "last_modified": properties.last_modified.isoformat() if hasattr(properties, "last_modified") else None,
                    "lease_status": properties.lease.status if hasattr(properties, "lease") else None
                }
                
                # List a few blobs
                blobs = list(container_client.list_blobs(maxresults=3))
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



class DataStorage:
    def __init__(self):
        self.container_name = azure_container_name

    async def save_dataframe_to_azure(
        self,
        dataframe: pd.DataFrame,
        file_name: str,
        blob_service_client: Optional[BlobServiceClient] = None
    ) -> bool:
        """
        Save DataFrame to Azure Data Lake Storage.

        Args:
            dataframe: DataFrame to save
            file_name: Name of the file in storage
            blob_service_client: Optional client for connection

        Returns:
            True if successful, False otherwise
        """
        try:
            # Initialize blob service client if not provided
            if blob_service_client is None:
                blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

            # Create container if it doesn't exist
            blob_service_client.create_container(self.container_name)

            # Convert DataFrame to CSV buffer
            csv_buffer = io.StringIO()
            dataframe.to_csv(csv_buffer, index=False)

            # Upload to Azure Data Lake
            blob_client = blob_service_client.get_blob_client(
                container=self.container_name,
                blob=file_name
            )
            blob_client.upload_text(csv_buffer.getvalue())

            return True

        except Exception as e:
            logger.error(f"Error saving data to Azure: {str(e)}")
            return False

    async def get_dataframe_from_azure(
        self,
        file_name: str,
        blob_service_client: Optional[BlobServiceClient] = None
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve DataFrame from Azure Data Lake Storage.

        Args:
            file_name: Name of the file in storage
            blob_service_client: Optional client for connection

        Returns:
            DataFrame if successful, None otherwise
        """
        try:
            # Initialize blob service client if not provided
            if blob_service_client is None:
                blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

            blob_client = blob_service_client.get_blob_client(
                container=self.container_name,
                blob=file_name
            )

            # Read blob content
            blob_data = blob_client.download_blob()
            csv_data = blob_data.readall().decode('utf-8')

            return pd.read_csv(io.StringIO(csv_data))

        except Exception as e:
            logger.error(f"Error retrieving data from Azure: {str(e)}")
            return None