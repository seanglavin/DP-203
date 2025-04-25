from typing import Optional, List, Dict, Any, Union
import io
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError
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

def get_petfinder_storage_client():
    """
    Factory function that returns storage client

    """
    # Return client
    return AzureDataStorageClient(
        connection_string=settings.AZURE_STORAGE_CONNECTION_STRING,
        container_name="petfinder"
    )

def get_mtg_storage_client():
    """
    Factory function that returns storage client

    """
    # Return client
    return AzureDataStorageClient(
        connection_string=settings.AZURE_STORAGE_CONNECTION_STRING,
        container_name="mtg"
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

    async def list_files(self, prefix: Optional[str] = None) -> List[str]:
        """
        List all file names in the container matching the prefix.

        Args:
            prefix: Optional filename prefix filter

        Returns:
            List of blob names (strings).
        """
        blob_names = []
        try:
            # Ensure container exists
            await self.ensure_container_exists()

            logger.debug(f"Listing all blobs with prefix: '{prefix}' in container '{self.container_name}'")
            # Iterate through the ItemPaged iterator to get all blobs
            blob_list = self.container_client.list_blobs(name_starts_with=prefix)
            blob_names = [blob.name for blob in blob_list] # Extract just the name

            logger.info(f"Found {len(blob_names)} blobs matching prefix '{prefix}'.")
            return blob_names

        except ResourceNotFoundError:
            logger.error(f"Container '{self.container_name}' not found.")
            return [] # Return empty list if container not found
        except Exception as e:
            logger.exception(f"Error listing files in container with prefix '{prefix}': {e}")
            # Depending on desired behavior, you might re-raise or return empty
            return [] # Return empty list on other errors


    # --- 3 in 1 upload method for csv, parquet, or json
    async def upload_data(self, data: Any, file_name: str, file_type: str) -> bool:
        """
        Upload data to Azure Storage based on the file type.

        Args:
            data: Data to store (Pandas DataFrame for CSV/Parquet, or any Python object for JSON)
            file_name: Path in Azure Blob Storage
            file_type: Type of file ('csv', 'parquet', 'json')

        Returns:
            True if successful, False otherwise.
        """
        try:
            # Ensure container exists
            await self.ensure_container_exists()

            # Perform upload based on file type
            if file_type == 'csv':
                buffer = io.StringIO()
                data.to_csv(buffer, index=False)
                blob_client = self.get_blob_client(file_name)
                blob_client.upload_blob(buffer.getvalue(), overwrite=True)
            elif file_type == 'parquet':
                parquet_buffer = io.BytesIO()
                data.to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy")
                parquet_buffer.seek(0)
                blob_client = self.get_blob_client(file_name)
                blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)
            elif file_type == 'json':
                json_string = json.dumps(data, indent=2, default=str)
                json_bytes = json_string.encode('utf-8')
                blob_client = self.get_blob_client(file_name)
                blob_client.upload_blob(json_bytes, overwrite=True)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            return True

        except Exception as e:
            logger.error(f"Error uploading {file_type.capitalize()} data: {str(e)}")
            raise


    # --- 3 in 1 read method for csv, parquet, or json
    async def read_data(self, file_name: str, file_type: str) -> Optional[Union[pd.DataFrame, Any]]:
        """
        Read data from Azure Storage based on the file type.

        Args:
            file_name: Path to the file in Azure Blob Storage
            file_type: Type of file ('csv', 'parquet', 'json')

        Returns:
            Pandas DataFrame for CSV/Parquet, or Python object (list/dict) for JSON, or None if file doesn't exist.
        """
        try:
            # Ensure container exists
            await self.ensure_container_exists()

            # Perform read based on file type
            blob_client = self.get_blob_client(file_name)
            if file_type == 'csv':
                blob_data = blob_client.download_blob().readall()
                buffer = io.StringIO(blob_data.decode('utf-8'))
                return pd.read_csv(buffer)
            elif file_type == 'parquet':
                blob_data = blob_client.download_blob().readall()
                buffer = io.BytesIO(blob_data)
                return pd.read_parquet(buffer, engine="pyarrow")
            elif file_type == 'json':
                blob_data = blob_client.download_blob().readall()
                return json.loads(blob_data.decode('utf-8'))
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

        except ResourceNotFoundError:
            logger.warning(f"{file_type.capitalize()} file {file_name} not found.")
            return None

        except Exception as e:
            logger.error(f"Error reading {file_type.capitalize()} data from {file_name}: {str(e)}")
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


    async def upload_json_data(self, data: Any, file_name: str) -> bool:
        """
        Upload Python data (list, dict) as a JSON file to Azure Storage.

        Args:
            data: The Python object (list or dict) to store.
            file_name: Path in Azure Blob Storage (should end with .json).

        Returns:
            True if successful, False otherwise.
        """
        if not file_name.endswith(".json"):
             logger.warning(f"File name '{file_name}' does not end with .json. Proceeding, but convention is recommended.")
        try:
            # Ensure container exists
            await self.ensure_container_exists()

            # Convert Python object to JSON string
            # Use default=str to handle potential non-serializable types like datetime
            json_string = json.dumps(data, indent=2, default=str)
            json_bytes = json_string.encode('utf-8')

            # Upload to storage
            blob_client = self.get_blob_client(file_name)
            logger.info(f"Uploading JSON data to {file_name}...")
            # Consider adding timeout like in parquet upload if files can be large
            upload_timeout_seconds = 300
            blob_client.upload_blob(
                json_bytes,
                overwrite=True,
                timeout=upload_timeout_seconds
            )
            logger.info(f"Successfully uploaded JSON data to {file_name}")
            return True

        except TypeError as te:
             logger.error(f"JSON Serialization Error for {file_name}: {te}. Check data types.")
             raise
        except Exception as e:
            logger.error(f"Error uploading JSON data to {file_name}: {str(e)}")
            raise


    async def read_json_data(self, file_name: str) -> Optional[Any]:
        """
        Read a JSON file from Azure Storage into a Python object.

        Args:
            file_name: Path to the JSON file in Azure Blob Storage.

        Returns:
            Python object (list or dict) containing the data, or None if the file doesn't exist or is invalid JSON.
        """
        if not file_name.endswith(".json"):
             logger.warning(f"Attempting to read non-JSON file extension as JSON: '{file_name}'")
        try:
            # Ensure container exists (optional, depends if you expect it)
            # await self.ensure_container_exists()

            # Get blob client
            blob_client = self.get_blob_client(file_name)

            # Check if blob exists
            if not blob_client.exists():
                logger.warning(f"JSON file {file_name} does not exist")
                return None

            # Download blob content
            logger.debug(f"Downloading JSON data from {file_name}...")
            blob_data = blob_client.download_blob().readall()
            logger.debug(f"Downloaded {len(blob_data)} bytes from {file_name}.")

            # Decode and parse JSON
            json_string = blob_data.decode('utf-8')
            data = json.loads(json_string)
            logger.info(f"Successfully read and parsed JSON data from {file_name}.")
            return data

        except json.JSONDecodeError as jde:
             logger.error(f"Error decoding JSON from {file_name}: {jde}")
             return None # Indicate failure to parse
        except ResourceNotFoundError: # More specific catch if exists() check is removed
             logger.warning(f"JSON file {file_name} not found (ResourceNotFoundError).")
             return None
        except Exception as e:
            logger.error(f"Error reading JSON data from {file_name}: {str(e)}")
            raise # Re-raise other unexpected errors


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

            # # logging block
            # if not blob_data:
            #     logger.info(f"Read blob parquet data failed for: {file_name}")
            # logger.info(f"Read blob parquet data success for: {file_name}")
            
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