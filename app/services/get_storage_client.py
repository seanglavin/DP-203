from logger_config import logger
from app.config_settings import settings
from app.services.data_storage import AzureDataStorageClient


def get_storage_client():
    """
    Factory function that returns storage client

    """
    # Return client
    return AzureDataStorageClient(
        connection_string=settings.AZURE_STORAGE_CONNECTION_STRING,
        container_name=settings.AZURE_STORAGE_CONTAINER_NAME
    )