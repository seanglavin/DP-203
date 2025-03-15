from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from app.services.data_storage import AzureDataStorageClient
from app.config_settings import settings
from app.endpoints import sports, storage
from logger_config import logger


load_dotenv()


app = FastAPI(
    title="Sports Data ETL API",
    description="API for fetching sports data and processing it through an ETL pipeline to Azure",
    version="0.1.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(sports.router, prefix="/api/sports", tags=["sports"])
app.include_router(storage.router, prefix="/api/storage", tags=["storage"])

@app.get("/")
async def root():
    return {"message": "Welcome to the Sports Data ETL API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/test/adls-connection")
async def test_adls_connection():
    """
    Tests connection to Azure Data Lake Storage using connection string
    Returns connection status and details
    """
    try:
        logger.info("Testing connection to Azure Data Lake Storage")
        
        # Get configuration from environment variables or settings
        connection_string = settings.AZURE_STORAGE_CONNECTION_STRING
        container_name = settings.AZURE_STORAGE_CONTAINER_NAME
        
        # Validation
        if not connection_string:
            return {
                "success": False,
                "message": "Missing required Azure Storage connection string",
                "details": "AZURE_STORAGE_CONNECTION_STRING must be provided in settings"
            }
        
        # Initialize the storage client
        storage_client = AzureDataStorageClient(
            connection_string=connection_string,
            container_name=container_name
        )

        # Call the test function
        connection_result = await storage_client.check_connection()
        
        # Add a message to the result
        if connection_result["success"]:
            connection_result["message"] = "Successfully connected to Azure Data Lake Storage"
            if container_name and connection_result["connection_details"].get("container_exists"):
                connection_result["message"] += f" and container '{container_name}'"
        else:
            connection_result["message"] = "Failed to connect to Azure Data Lake Storage"
            if "connection_error" in connection_result["connection_details"]:
                connection_result["message"] += f": {connection_result['connection_details']['connection_error']}"
        
        return connection_result
    
    except Exception as e:
        logger.error(f"Error testing ADLS connection: {str(e)}")
        return {
            "success": False,
            "message": "Azure Data Lake Storage connection test failed",
            "error": str(e)
        }