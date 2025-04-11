import httpx
import pandas as pd
import numpy as np
import random
import json
from typing import List, Dict
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  # Handles month calculations
from fastapi import HTTPException, APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from app.services.azure_storage_client import AzureDataStorageClient, get_petfinder_storage_client
from app.models.azure_storage_models import FileListResponse

from logger_config import logger


PETFINDER_API_KEY="IElYS4DLQKCSuQDrNotNfGXZU6H7Fb90CH33Q4dy6UlNBewHaa"
PETFINDER_API_SECRET="YJuG8eAIahaco7yp4PG7xzH9qOCc44bcsafLoSSd"
PETFINDER_API_URL="https://api.petfinder.com/v2/animals"


router = APIRouter()


# Use dependency injection for the storage client
async def get_storage():
    """Dependency to get storage client"""
    return get_petfinder_storage_client()

# Helper to flatten nested petfinder data
def flatten_pet_data(pet):
    """Flatten nested Petfinder pet data."""
    flattened = {
        "id": pet["id"],
        "organization_id": pet["organization_id"],
        "url": pet["url"],
        "type": pet["type"],
        "species": pet["species"],
        "age": pet["age"],
        "gender": pet["gender"],
        "size": pet["size"],
        "coat": pet["coat"],
        "name": pet["name"],
        "description": pet["description"],
        "status": pet["status"],
        "published_at": pet["published_at"],
        "distance": pet["distance"],
        # Flattening breeds
        "primary_breed": pet["breeds"]["primary"],
        "secondary_breed": pet["breeds"]["secondary"],
        "mixed_breed": pet["breeds"]["mixed"],
        "unknown_breed": pet["breeds"]["unknown"],
        # Flattening colors
        "primary_color": pet["colors"]["primary"],
        "secondary_color": pet["colors"]["secondary"],
        "tertiary_color": pet["colors"]["tertiary"],
        # Flattening attributes
        "spayed_neutered": pet["attributes"]["spayed_neutered"],
        "house_trained": pet["attributes"]["house_trained"],
        "declawed": pet["attributes"]["declawed"],
        "special_needs": pet["attributes"]["special_needs"],
        "shots_current": pet["attributes"]["shots_current"],
        # Flattening environment
        "children_friendly": pet["environment"]["children"],
        "dogs_friendly": pet["environment"]["dogs"],
        "cats_friendly": pet["environment"]["cats"],
        # Flattening tags as a comma-separated string
        "tags": ", ".join(pet["tags"]) if pet["tags"] else None,
        # Flattening photos (just one photo here for simplicity)
        "photo_url": pet["photos"][0].get("medium") if pet.get("photos") and len(pet["photos"]) > 0 else None,
        # Flattening contact info
        "contact_email": pet["contact"]["email"],
        "contact_phone": pet["contact"]["phone"],
        "contact_city": pet["contact"]["address"]["city"],
        "contact_state": pet["contact"]["address"]["state"],
        "contact_postcode": pet["contact"]["address"]["postcode"],
        "contact_country": pet["contact"]["address"]["country"]
    }
    return flattened


async def get_access_token():

    """
    Get a new access token from the PetFinder API using client credentials.

    Returns:
        dict: Access token response in JSON format.
    """

    token_url = "https://api.petfinder.com/v2/oauth2/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": PETFINDER_API_KEY,
        "client_secret": PETFINDER_API_SECRET
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(token_url, data=data)
        logger.info(response.json())

    if response.status_code == 200:
        return response.json()
    else:
        return {"error": f"Error: {response.status_code}", "message": response.text}



async def fetch_pets_for_month(client, start_date):
    """Fetch pets for a specific month, handling pagination."""
    # Get a new access token
    access_token_data = await get_access_token()
    access_token = access_token_data.get("access_token", "")

    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    pets = []
    end_date = (start_date + relativedelta(months=1)).replace(day=1)  # First day of next month

    after = start_date.strftime("%Y-%m-%dT00:00:00Z")
    before = end_date.strftime("%Y-%m-%dT00:00:00Z")

    page = 1
    async with httpx.AsyncClient() as client:
        while True:
            try:
                response = await client.get(
                    PETFINDER_API_URL, 
                    headers=headers, 
                    params={
                        "before": before,
                        "after": after,
                        "sort": "recent",
                        "page": page,
                        "limit": 100,
                        "location": "Edmonton, AB",
                        "distance": 500
                    },
                    timeout=10
                )
                response.raise_for_status()  # Raise an error for HTTP failures

                data = response.json()
                animals = data.get("animals", [])
                if not animals:
                    break  # Stop if no more pets

                pets.extend([flatten_pet_data(pet) for pet in animals])
                page += 1

            except httpx.TimeoutException:
                logger.info(f"⚠ Timeout for {start_date.strftime('%B %Y')} page {page}. Retrying...")
                continue  # Retry on timeout

            except httpx.HTTPStatusError as e:
                logger.info(f"❌ HTTP Error {e.response.status_code} for {start_date.strftime('%B %Y')} page {page}")
                break  # Stop retrying if it's an HTTP error

        return pets


async def fetch_recent_pets(months_of_data):
    """Fetch pets from the most recent N months."""
    all_pets = []
    today = datetime.now().replace(day=1)  # Start from the 1st of the current month

    async with httpx.AsyncClient() as client:
        for i in range(months_of_data):
            month_start = today - relativedelta(months=i)
            pets = await fetch_pets_for_month(client, month_start)

            all_pets.extend(pets)
            all_pets_dataframe = pd.DataFrame(all_pets)

            logger.info(f"Fetched {len(pets)} pets for {month_start.strftime('%B %Y')}")
        logger.info(all_pets_dataframe.head())

    return all_pets_dataframe





# Root
@router.get("/")
async def petfinder_root():
    logger.info("message: petfinder root")
    return {"message": "petfinder root"}


@router.get("/files", response_model=List[FileListResponse])
async def list_parquet_files(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    List all Parquet files in the Azure Storage container.
    """
    logger.info("Received GET request | list_parquet_files")
    try:
        # Filter for .parquet files
        files = await storage_client.list_files()
        parquet_files = [f for f in files if f["name"].endswith(".parquet")]

        logger.info(f"Found {len(parquet_files)} Parquet files")
        return parquet_files

    except Exception as e:
        logger.error(f"Error listing Parquet files: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pets")
async def fetch_petfinder_pets(storage_client: AzureDataStorageClient = Depends(get_storage)):
    try:
        pets_data = await fetch_recent_pets(months_of_data=24)
        if pets_data.empty:
            return {"message": "No pet data found."}
        
        # Convert  to DataFrame
        df = pd.DataFrame(pets_data)

        # Ensure published_at is a datetime field
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

        # Filter out records without a photo
        df = df[df["photo_url"].notna() & df["photo_url"].str.strip().ne("")]

        # Partition data by month
        for month, month_df in df.groupby(df["published_at"].dt.to_period("M")):
            month_str = month.strftime("%Y-%m")  # Format: "YYYY-MM"
            file_name = f"petfinder_{month_str}.parquet"

            # Upload partitioned data
            success = await storage_client.upload_data_as_parquet(data=month_df, file_name=file_name)

            if not success:
                logger.error(f"Failed to upload data for {month_str}")

        return {"message": "Data uploaded successfully", "partitions": df["published_at"].dt.to_period("M").nunique()}

    except Exception as e:
        logger.exception("Failed to upload data as parquet")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    

@router.get("/pets/read", response_model=List[Dict])
async def read_petfinder_pets_from_storage(
    storage_client: AzureDataStorageClient = Depends(get_storage),
    pet_type: str = None
    ):
    """
    Read and return stored pet data from Azure Storage, with optional filters.
    - `pet_type`: Filter by pet type (e.g., 'Cat', 'Dog').
    """
    try:
        # List all stored Parquet files in the container
        files = await storage_client.list_files()
        parquet_files = [f for f in files if f["name"].endswith(".parquet")]

        logger.info(f"parquet_files = {parquet_files}")

        if not parquet_files:
            return {"message": "No pet data found in storage."}

        # --- if you're passing multiple files at once, map them properly
        # df_list = await asyncio.gather(
        #     *[storage_client.read_parquet_data(f["name"]) for f in parquet_files]
        # )

        # Read all Parquet files
        all_data = []
        for file in parquet_files:
            file_name = file["name"]
            df = await storage_client.read_parquet_data(file_name)

            if df is None or df.empty:
                logger.info(f"❌ Empty DataFrame in {file_name}")

            else:
                logger.info(f"✅ Successfully read {len(df)} rows from {file_name}")
                all_data.append(df)

        # Merge all data into a single DataFrame
        full_df = pd.concat(all_data, ignore_index=True)

        # Apply the 'pet_type' filter if provided
        if pet_type:
            full_df = full_df[full_df["type"].str.lower() == pet_type.lower()]

        # Convert to dict for JSON response
        result = full_df.to_dict(orient="records")
   
        logger.info(f"Read total {len(full_df)} pet records.")
        logger.info(f"Sample data: {full_df.head()}")
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to read pet data from storage")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    


@router.get("/pets/gameboards")
async def get_game_boards(
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """Read the generated gameboards file and return the gameboards as JSON."""
    try:
        file_name = "gameboards.parquet"

        # Check if the file exists
        files = await storage_client.list_files()
        if file_name not in [f["name"] for f in files]:
            return {"message": "Gameboards file not found. Please generate it first."}

        # Read the Parquet file
        gameboard_df = await storage_client.read_parquet_data(file_name)

        if gameboard_df is None or gameboard_df.empty:
            return {"message": "Gameboards file is empty."}

        # Convert timestamps back to string if needed
        if "published_at" in gameboard_df.columns:
            gameboard_df["published_at"] = gameboard_df["published_at"].astype(str)

        # Reconstruct gameboards from the DataFrame
        game_boards = []
        for gameboard_id, board_df in gameboard_df.groupby("gameboard_id"):
            game_boards.append({
                f"pet{j+1}": pet for j, pet in enumerate(board_df.to_dict(orient="records"))
            })

        return {"gameboards": game_boards}

    except Exception as e:
        logger.exception("Failed to read gameboards file")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    

@router.post("/pets/gameboards/generate")
async def generate_game_boards_file(
    type: str = Query(None),
    age: str = Query(None),
    gender: str = Query(None),
    name: str = Query(None),
    size: str = Query(None),
    primary_breed: str = Query(None),
    primary_color: str = Query(None),
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """Generate a single Parquet file containing gameboards with unique pets (up to 12)."""
    try:
        # List available parquet files
        files = await storage_client.list_files()
        parquet_files = [f for f in files if f["name"].endswith(".parquet")]

        if not parquet_files:
            return {"message": "No pet data found in storage."}

        # Read and merge all parquet files
        all_data = []
        for file in parquet_files:
            df = await storage_client.read_parquet_data(file["name"])
            if df is not None and not df.empty:
                all_data.append(df)

        if not all_data:
            return {"message": "No valid pet data available."}

        # Combine all data
        full_df = pd.concat(all_data, ignore_index=True)

        # Apply filters
        filters = {
            "type": type,
            "age": age,
            "gender": gender,
            "name": name,
            "size": size,
            "primary_breed": primary_breed,
            "primary_color": primary_color,
        }
        for column, value in filters.items():
            if value:
                full_df = full_df[full_df[column] == value]

        # Ensure published_at is a string
        if "published_at" in full_df.columns:
            full_df["published_at"] = pd.to_datetime(full_df["published_at"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Ensure there are enough pets
        total_pets = len(full_df)
        if total_pets < 5:
            return {"message": "Not enough unique pets available to create even one game board."}

        # Shuffle the pets for randomness
        shuffled_pets = full_df.sample(frac=1, random_state=42).reset_index(drop=True)

        # Determine the number of possible game boards
        num_boards = total_pets // 5  # Each board needs 5 pets

        # Split into game boards
        game_boards = []
        for i in range(num_boards):
            board_pets = shuffled_pets.iloc[i * 5: (i + 1) * 5].copy()
            game_boards.append(board_pets)

        # Flatten gameboards into a single DataFrame for saving
        gameboard_df = pd.concat(game_boards, keys=range(len(game_boards)), names=["gameboard_id"]).reset_index(level=0)

        # Save gameboards to a single Parquet file
        file_name = "gameboards.parquet"
        success = await storage_client.upload_data_as_parquet(data=gameboard_df, file_name=file_name)

        if not success:
            logger.error("Failed to upload gameboards file.")
            return {"message": "Failed to generate gameboards file."}

        return {"message": "Gameboards file generated successfully", "file_name": file_name}

    except Exception as e:
        logger.exception("Failed to generate gameboards file")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    


