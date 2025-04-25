from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from dotenv import load_dotenv
from datetime import datetime


from app.models.azure_storage_models import ConnectionTestResponse, StorageConnectionDetails
from app.services.azure_storage_client import AzureDataStorageClient
from app.config_settings import settings
from app.endpoints import azure_storage_endpoints, petfinder_api_endpoints, source_data_endpoints, magic_api_endpoints
from logger_config import logger


load_dotenv()


app = FastAPI(
    title="SKG Data ETL API",
    description="API for data and processing it through an ETL pipeline to Azure",
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
app.include_router(source_data_endpoints.router, prefix="/api/source", tags=["source"])
app.include_router(azure_storage_endpoints.router, prefix="/storage", tags=["storage"])
app.include_router(petfinder_api_endpoints.router, prefix="/api/petfinder", tags=["petfinder"])
app.include_router(magic_api_endpoints.router, prefix="/api/mtg", tags=["mtg"])

@app.get("/")
async def root():
    return {"message": "Welcome to the Sports Data ETL API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/test/adls-connection", response_model=ConnectionTestResponse)
async def test_adls_connection():
    """
    Tests connection to Azure Data Lake Storage using connection string
    Returns connection status and details
    """
    try:
        logger.info("Testing connection to Azure Data Lake Storage...")
        
        # Get configuration from environment variables or settings
        connection_string = settings.AZURE_STORAGE_CONNECTION_STRING
        container_name = settings.AZURE_STORAGE_CONTAINER_NAME

        logger.info(f"connection_string: {connection_string}")
        logger.info(f"container_name: {container_name}")

        # Initialize the storage client
        storage_client = AzureDataStorageClient(
            connection_string = connection_string,
            container_name = container_name
        )

        # Call the test function
        connection_result = await storage_client.check_connection()
        
        # Prepare response model
        response = ConnectionTestResponse(
            success=connection_result["success"],
            message="Azure Data Lake Storage connection test successful" if connection_result["success"] else "Azure Data Lake Storage connection test failed",
            timestamp=connection_result["timestamp"],
            connection_details=connection_result["connection_details"]
        )
        return response
    
    except Exception as e:
        error_response = ConnectionTestResponse(
            success=False,
            message="Azure Data Lake Storage connection test failed",
            timestamp=datetime.now().isoformat(),
            connection_details=StorageConnectionDetails(
                connection_valid=False,
                connection_error=str(e)
            )
        )
        return error_response
