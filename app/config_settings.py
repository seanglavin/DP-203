import os

class Settings:
    # thesportsdb free api
    THESPORTSDB_FREE_API_BASE: str = os.getenv("THESPORTSDB_FREE_API_BASE")
    RAPID_API_HOCKEY_URL_BASE: str = os.getenv("RAPID_API_HOCKEY_URL_BASE")
    RAPID_API_NBA_URL_BASE: str = os.getenv("RAPID_API_NBA_URL_BASE")
    RAPID_API_NBA_FREE_URL_BASE: str = os.getenv("RAPID_API_NBA_FREE_URL_BASE")
    RAPID_API_KEY: str = os.getenv("RAPID_API_KEY")

    # Azure Storage
    AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    AZURE_STORAGE_CONTAINER_NAME: str = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

    USE_MOCK_STORAGE: str = os.getenv("USE_MOCK_STORAGE")
    USE_MOCK_DATA: str = os.getenv("USE_MOCK_DATA")

    # Project Root
    PROJECT_ROOT: str = os.getenv("PROJECT_ROOT")
    TEST_DATA_DIR: str = os.getenv("TEST_DATA_DIR")


settings = Settings()