import os

class Settings:
    # thesportsdb free api
    THESPORTSDB_FREE_API_BASE: str = os.getenv("THESPORTSDB_FREE_API_BASE")

    # Azure Storage
    AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_STORAGE_CONTAINER_NAME: str = os.getenv("AZURE_STORAGE_CONTAINER_NAME")


settings = Settings()