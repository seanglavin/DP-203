from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from datetime import datetime

from app.models.azure_storage import ConnectionTestResponse, StorageConnectionDetails
from app.services.data_storage import AzureDataStorageClient
from app.config_settings import settings
from app.endpoints import source_nba_data
from logger_config import logger, logger_api_response


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
# app.include_router(sports.router, prefix="/api/sports", tags=["sports"])
# app.include_router(storage.router, prefix="/api/storage", tags=["storage"])
app.include_router(source_nba_data.router, prefix="/api/source", tags=["source"])

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

        # Log response after getting results
        logger_api_response(response)
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
        logger_api_response(error_response, "error")
        return error_response


# --- Continue this Convenience endpoint
# @app.post("/extract-and-save/{league}", response_model=LeagueSaveResponse, tags=["orchestration"])
# async def extract_and_save_league_orchestrated(
#     league: str, 
#     background_tasks: BackgroundTasks,
#     api_url: str = Depends(get_api_url)
# ):
#     """
#     Orchestration endpoint that calls the individual sports and storage endpoints
#     to extract and save data
    
#     Parameters:
#     - league: League to fetch and save data for (e.g., "nba", "nhl")
#     """
#     try:
#         logger.info(f"Orchestrating extract and save for {league.upper()} via API calls")
        
#         # Validate league parameter
#         if league.lower() not in ["nba", "nhl"]:
#             raise HTTPException(status_code=400, detail=f"Unsupported league: {league}")
        
#         # Step 1: Call the league data endpoint to get the data
#         async with httpx.AsyncClient() as client:
#             teams_response = await client.get(f"{api_url}/api/sports/{league.lower()}/teams")
            
#             if teams_response.status_code != 200:
#                 error_detail = teams_response.json().get("detail", f"Error fetching {league} data")
#                 raise HTTPException(status_code=teams_response.status_code, detail=error_detail)
            
#             teams_data = teams_response.json()
            
#             if teams_data.get("count", 0) == 0:
#                 return LeagueSaveResponse(
#                     success=False,
#                     message=f"No {league.upper()} data found to save",
#                     league=league.lower(),
#                     filename="",
#                     rows=0
#                 )
            
#             # Step 2: Call the save endpoint with the fetched data
#             save_response = await client.post(
#                 f"{api_url}/api/storage/save/{league.lower()}"
#             )
            
#             if save_response.status_code != 200:
#                 error_detail = save_response.json().get("detail", f"Error saving {league} data")
#                 raise HTTPException(status_code=save_response.status_code, detail=error_detail)
            
#             save_data = save_response.json()
            
#             # Return the save response
#             return LeagueSaveResponse(
#                 success=save_data.get("success", False),
#                 message=save_data.get("message", ""),
#                 league=league.lower(),
#                 rows=save_data.get("rows", 0),
#                 filename=save_data.get("filename", "")
#             )
    
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error in orchestrated extract and save for {league}: {str(e)}")
#         raise HTTPException(status_code=500, detail=f"Error in orchestrated operation: {str(e)}")