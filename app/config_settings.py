import os

class Settings:
    # thesportsdb free api
    THESPORTSDB_FREE_API_BASE: str = os.getenv("THESPORTSDB_FREE_API_BASE")

    # Azure Storage
    AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_STORAGE_CONTAINER_NAME: str = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

    USE_MOCK_STORAGE: str = os.getenv("USE_MOCK_STORAGE")
    USE_MOCK_DATA: str = os.getenv("USE_MOCK_DATA")

    # Project Root
    PROJECT_ROOT: str = os.getenv("PROJECT_ROOT")
    TEST_DATA_DIR: str = os.getenv("TEST_DATA_DIR")


settings = Settings()