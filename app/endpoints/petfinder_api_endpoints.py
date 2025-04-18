import httpx
import pandas as pd
import numpy as np
import random
import json
import re
import html
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  # Handles month calculations
from fastapi import HTTPException, APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from app.services.azure_storage_client import AzureDataStorageClient, get_petfinder_storage_client
from app.models.azure_storage_models import FileListResponse, ParquetReadDataResponse, ParquetMergeResponse

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
        "type": pet["type"],
        "species": pet["species"],
        "name": pet["name"],
        "age": pet["age"],
        "gender": pet["gender"],
        "size": pet["size"],
        "coat": pet["coat"],
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
        # "declawed": pet["attributes"]["declawed"],
        "special_needs": pet["attributes"]["special_needs"],
        # "shots_current": pet["attributes"]["shots_current"],
        # Flattening environment
        "children_friendly": pet["environment"]["children"],
        "dogs_friendly": pet["environment"]["dogs"],
        "cats_friendly": pet["environment"]["cats"],
        # Flattening tags as a comma-separated string
        "tags": ", ".join(pet["tags"]) if pet["tags"] else None,
        # Flattening photos (just one photo here for simplicity)
        "photo_url": pet["photos"][0].get("medium") if pet.get("photos") and len(pet["photos"]) > 0 else None,
        "photo_url_small": pet["photos"][0].get("small") if pet.get("photos") and len(pet["photos"]) > 0 else None,
        # Flattening contact info
        "contact_email": pet["contact"]["email"],
        "contact_phone": pet["contact"]["phone"],
        "contact_city": pet["contact"]["address"]["city"],
        "contact_state": pet["contact"]["address"]["state"],
        "contact_postcode": pet["contact"]["address"]["postcode"],
        "contact_country": pet["contact"]["address"]["country"],
        "organization_id": pet["organization_id"],
        "url": pet["url"]
    }
    return flattened


# --- Enhanced Name Cleaning Function ---
def clean_pet_name(name: Optional[str]) -> Optional[str]:
    """
    Clean and standardize pet name values, removing common extra info.

    Args:
        name (str): The raw pet name to be cleaned

    Returns:
        str: A cleaned version of the name, or None if it's invalid/empty after cleaning.
    """
    if pd.isna(name) or not isinstance(name, str) or not name.strip():
        return None

    # 1. Decode HTML entities like &amp;
    cleaned = html.unescape(name)

    # 2. Remove common prefixes/suffixes (case-insensitive)
    # Order matters: More specific patterns first
    prefixes_suffixes = [
        # --- Prefixes ---
        r'^\?*\s*Meet\s+', # "? Meet "
        # Updated to include POST
        r'^\*+\s*COURTESY (?:LISTING|POST)\*+\s*', # "***COURTESY LISTING/POST*** ", etc.
        r'^z\s*West Coast Paws\s*', # "z West Coast Paws "
        r'^\d+[\s-]*', # Leading numbers/IDs like "34944- " or "35638 -"

        # --- Suffixes (more specific first) ---
        # Patterns with hyphens, em/en dashes (–), or commas indicating extra info
        r'\s*[–-]\s*\d+\s+Years?\s+Old.*$', # " – 4 Years Old..." (using [–-] for en/em dash or hyphen)
        r'\s*[–-]\s*In\s+Foster\s+Care.*$', # " – In Foster Care"
        r'\s*[–-]\s*\$?\d+\s*(?:SHY CAT SPECIAL|fee)\b.*$', # Trailing fee/special info after dash
        r'\s*[–-]\s*Courtesy Listing(?: see info)?\.?$', # Trailing courtesy listing info
        r'\s*[–-]\s*Medical Needs$',
        r'\s*[–-]\s*Submit Application to meet$',
        r'\s*[–-]\s*Meeting Details in Bio$',
        r'\s*[–-]\s*local .* adoption required.*$',
        r'\s*[–-]\s*Tripod$',
        r'\s*[–-]\s*1 eyed$',
        r'\s*[–-]\s*Your .* Buddy!?$', # " – Your Perfect Adventure Buddy!"
        r'\s*[–-]\s*The .* Queen!?$', # " – The Stunning, Spirited Snow Queen!"
        r'\s*[–-]\s*A Resilient, Loving Companion!?$', # " – A Resilient, Loving Companion!"
        r'\s*[–-]\s*Bosley\'s on .*$', # Location info
        r'\s*[–-]\s*Vedder Road .*$', # Location info

        # Patterns with commas
        r'\s*,\s*(?:GSD|mix|mellow|special needs|read bio)\b.*$', # Trailing breed/desc/needs after comma
        r'\s*,\s*\$\d+\s+fee\b.*$', # Trailing fee after comma

        # Patterns with asterisks or specific phrases
        r'\s+\*(?:read|see|check)\b.*$', # Handles " *read bio", " *see description" etc.
        r'\*+\s*READ ENTIRE DESCRIPTION\s*\*+$', # Trailing "READ..."
        r'\*+\s*SPECIAL NEEDS\s*\*+$', # Trailing "SPECIAL NEEDS..."
        r'\*+\s*WAITING FOR A FAMILY.*\s*\*+$', # Trailing "WAITING..."
        r'\*+\s*(?:read\s+bio)\s*\*+', # Phrases within asterisks

        # All caps suffixes (like Marvin NOW AT...)
        r'\s+([A-Z]{2,}\s*)+$', # General trailing all caps words

        # Location/Misc patterns
        r'\s*in British Columbia$',
        r'\s*\(?EDMONTON, AB\)?$',
        r'\s*\(?CALGARY, AB\)?$',
        r'\s*\(?Vancouver, BC\)?$',
        r'\s*\(?KELOWNA, BC\)?$',
        r'\s*\(?WA\)?$', # State abbreviations
        r'\s*\(?ID\)?$', # State abbreviations
        r'\s*\(?Armstrong\)?$', # Location
        r'\s*\(?ie: no cost\)?$', # Trailing "(ie: no cost)"
        r'\s*~\s*Crosspost for.*$',
        r'\s*\|\s*Sweet\s*\|\s*Shy\s*\|\s*Gentle$',
        r'\s+\d{3}-\d{3}-\d{4}$', # Trailing phone numbers like Knox
        r'\s*Generic (?:Medium|Large) Dog.*$',
        r'\s*7910-\d{2}$', # Specific patterns like Australian Log Runner
        r'\s*pending home$',

        # General cleanup patterns (less specific)
        r'\s*\.\.\.$', # Trailing ellipsis
        r'\?{2,}$', # Trailing multiple question marks
        r'\*{1,}$', # Trailing asterisks (after specific *phrases* are removed)
        r'\.{1,}$' # Trailing periods
    ]
    for pattern in prefixes_suffixes:
        # Apply each pattern sequentially
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE).strip()

    # 3. Remove content within parentheses and brackets (non-greedy)
    cleaned = re.sub(r'\(.*?\)', '', cleaned).strip()
    cleaned = re.sub(r'\[.*?\]', '', cleaned).strip()

    # 4. Split by common separators and take the first part (if not already handled by suffixes)
    # Include em/en dash –
    separators = [' – ', ' - ', ':', '~', '|', '\\', '/']
    for sep in separators:
        if sep in cleaned:
            parts = cleaned.split(sep, 1)
            if len(parts[0]) > 1 and not parts[0].lower() in ['a', 'the', 'pet']:
                 cleaned = parts[0].strip()

    # 5. Handle quotes (remove them)
    cleaned = re.sub(r'\"(.*?)\"', r'\1', cleaned)
    cleaned = re.sub(r'\`(.*?)\`', r'\1', cleaned)
    cleaned = cleaned.replace('"', '')
    # Remove remaining asterisks after specific patterns are handled
    cleaned = cleaned.replace('*', '').strip()

    # 6. Remove leading/trailing non-alphanumeric chars (allow internal hyphens/spaces)
    cleaned = re.sub(r'^[^a-zA-Z0-9]+', '', cleaned)
    cleaned = re.sub(r'[^a-zA-Z0-9\s\-]+$', '', cleaned) # Allow space/hyphen at end temporarily

    # 7. Final trim and check if the result is purely numeric or empty
    cleaned = cleaned.strip()
    if cleaned.isdigit() or not cleaned:
        return None

    # 8. Final whitespace cleanup
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # 9. Optional: Title Case
    # cleaned = cleaned.title()

    return cleaned if cleaned else None


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


# ---
async def merge_raw_data_and_save(storage_client: AzureDataStorageClient):
    """Merge all parquet files in raw_data/ into a single file."""
    try:
        # Get list of all parquet files
        files = await storage_client.list_files()

        # Filter for files in raw_data folder and with .parquet extension
        raw_data_files = [
            f for f in files
            if f["name"].startswith("raw_data/")
            and f["name"].endswith(".parquet")
        ]

        if not raw_data_files:
            return {"message": "No parquet files found in raw_data folder."}

        # Read all pet data
        all_pets = []
        for file in raw_data_files:
            df = await storage_client.read_parquet_data(file["name"])
            if df is not None and not df.empty:
                all_pets.append(df)

        if not all_pets:
            return {"message": "No valid pet data found."}

        # Merge all DataFrames
        merged_df = pd.concat(all_pets, ignore_index=True)

        # Ensure published_at is a datetime field
        merged_df["published_at"] = pd.to_datetime(merged_df["published_at"], errors="coerce")

        # Filter out records without photos (keep if either photo_url_small or photo_url exists)
        merged_df = merged_df[
            (merged_df["photo_url_small"].notna() & merged_df["photo_url_small"].str.strip().ne("")) |
            (merged_df["photo_url"].notna() & merged_df["photo_url"].str.strip().ne(""))
        ]

        # Save merged data to a new file
        folder_name = "merged_data"
        file_name = f"merged_pet_data.parquet"

        success = await storage_client.upload_data_as_parquet(
            data=merged_df,
            file_name=f"{folder_name}/{file_name}"
        )

        if not success:
            logger.error("Failed to upload merged file.")
            return {"message": "Failed to save merged file."}

        return {
            "success": True,
            "merged_file": f"{folder_name}/{file_name}",
            "total_records": len(merged_df),
            "sample_data": merged_df.head().to_dict('records')
        }

    except Exception as e:
        logger.exception("Failed to merge parquet files")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


# ---
async def create_game_boards_parquet_file(storage_client: AzureDataStorageClient):
    """
    Create and save a file containing 10 game boards, each with 5 random pets.
    """
    try:
        # Generate 10 game boards
        game_boards = []

        for _ in range(10):
            response = await get_random_pet_records(storage_client=storage_client)

            if not response["data"]:
                raise ValueError("No pets found")

            game_boards.append(response["data"])

        # Create DataFrame with the game boards data
        df = pd.DataFrame({
            "game_board": game_boards
        })

        # Save to parquet file
        file_name = "game_boards.parquet"

        success = await storage_client.upload_data_as_parquet(data=df, file_name=file_name)

        if not success:
            logger.error("Failed to upload game boards file.")
            return {"message": "Failed to save game boards."}

        return {
            "message": f"Created and saved {len(game_boards)} game boards to {file_name}",
            "game_board_count": len(game_boards)
        }

    except Exception as e:
        logger.exception("Failed to create game boards file")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


# ---
# ---
# --- Endpoints
# ---
# ---


# Root
@router.get("/")
async def petfinder_root():
    logger.info("message: petfinder root")
    return {"message": "petfinder root"}


# --- List parquet files in the container
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


# --- Fetch latest 24 months of petfinder pets withtin 500miles of Edmonton that have a photo and save to storage
@router.post("/pets/raw_data")
async def fetch_and_save_petfinder_pets_raw_data(storage_client: AzureDataStorageClient = Depends(get_storage)):
    try:
        pets_data = await fetch_recent_pets(months_of_data=24)
        if pets_data.empty:
            return {"message": "No pet data found."}
        
        # Convert  to DataFrame
        df = pd.DataFrame(pets_data)

        # Ensure published_at is a datetime field
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

        # Convert published_at to string format "YYYYMMDD"
        df["published_at"] = df["published_at"].dt.strftime("%Y%m%d")

        # Filter out records without a photo (keep if either photo_url_small or photo_url exists)
        df = df[
            (df["photo_url_small"].notna() & df["photo_url_small"].str.strip().ne("")) |
            (df["photo_url"].notna() & df["photo_url"].str.strip().ne(""))
        ]

        # Partition data by month
        for month, month_df in df.groupby(df["published_at"].str.slice(stop=6)):  # Group by YYYYMM
            month_str = month  # Format: "YYYYMM"
            folder_name = "raw_data"
            file_name = f"petfinder_{month_str}.parquet"

            # Upload partitioned data
            success = await storage_client.upload_data_as_parquet(data=month_df, file_name=f"{folder_name}/{file_name}")

            if not success:
                logger.error(f"Failed to upload data for {month_str}")

            logger.info(f"Data file saved: {folder_name}/{file_name}")

        return {"message": "Data uploaded successfully", "partitions": df["published_at"].str.slice(stop=6).nunique()}

    except Exception as e:
        logger.exception("Failed to upload data as parquet")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


# --- Read a file by name in raw_data/
@router.get("/pets/raw_data/{filename}", response_model=ParquetReadDataResponse)
async def get_parquet_raw_data(
    filename: str,
    num_samples: Optional[int] = 5,
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Read and return contents of a specific Parquet file.
    - `filename`: Name of the Parquet file to read
    """
    try:
        full_file_path = f"raw_data/{filename}"
        # Read the Parquet file from storage
        df = await storage_client.read_parquet_data(full_file_path)

        if df is None or df.empty:
            raise HTTPException(status_code=404, detail=f"File '{filename}' not found or empty")

        # Convert DataFrame to list of dictionaries for JSON serialization
        data = df.to_dict('records')

        return {
            "success": True,
            "count": len(data),
            "sample_data": data[:num_samples], # Show first 5 records as sample
            "data": data
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to read parquet file '{filename}'")
        raise HTTPException(
            status_code=500,
            detail=f"Internal Server Error: {str(e)}"
        )
    

# --- Merge the raw_data into a single merged_pet_data
@router.post("/pets/merged_data", response_model=ParquetMergeResponse)
async def merge_raw_data_parquet_files(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Merge all parquet files in raw_data/ into a single file.
    """
    try:
        response = await merge_raw_data_and_save(storage_client)

        return response

    except Exception as e:
        logger.exception("Failed to process parquet merge request")
        raise HTTPException(status_code=500, detail=str(e))
    

# --- Read file by name in merged_data/
@router.get("/pets/merged_data", response_model=ParquetReadDataResponse)
async def get_parquet_merged_data(
    pet_type: str = None,
    age: int = None,
    gender: str = None,
    size: str = None,
    # name: str = None,
    num_samples: int = 5,
    storage_client: AzureDataStorageClient = Depends(get_storage),
):
    """
    Read and return contents of a specific Parquet file with optional filtering.
    - `num_samples`: Number of sample records to return (default: 5)
    - `pet_type`: Filter by pet type
    - `age`: Filter by age
    - `gender`: Filter by gender
    - `size`: Filter by size
    - `name`: Filter by name
    """
    try:
        full_file_path = "merged_data/merged_pet_data.parquet"
        # Read the Parquet file from storage
        df = await storage_client.read_parquet_data(full_file_path)

        if df is None or df.empty:
            raise HTTPException(status_code=404, detail="File 'merged_pet_data.parquet' not found or empty")

        # Apply filters
        filtered_df = df
        if pet_type:
            filtered_df = filtered_df[filtered_df["type"] == pet_type]
        if age is not None:
            filtered_df = filtered_df[filtered_df["age"] == age]
        if gender:
            filtered_df = filtered_df[filtered_df["gender"] == gender]
        if size:
            filtered_df = filtered_df[filtered_df["size"] == size]
        # if name:
            # # Clean the input name
            # cleaned_name = clean_pet_name(name)

            # if cleaned_name:
            #     filtered_df = filtered_df[filtered_df["name"].str.contains(cleaned_name, na=False)]
        
        # Clean names in the resulting DataFrame
        # filtered_df["cleaned_name"] = filtered_df.apply(lambda row: clean_pet_name(row["name"]), axis=1)

        # Convert DataFrame to list of dictionaries for JSON serialization
        data = filtered_df.to_dict('records')

        return {
            "success": True,
            "count": len(data),
            "sample_data": data[:num_samples],  # Show first 5 records as sample
            "data": data
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to read parquet file 'merged_pet_data.parquet'")
        raise HTTPException(
            status_code=500,
            detail=f"Internal Server Error: {str(e)}"
        )


# --- Endpoint to get unique pet names from merged data
@router.get("/pets/merged_data/unique_names", response_model=List[str])
async def get_unique_pet_names(
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Retrieve a list of unique pet names from the merged Parquet file.
    """
    logger.info("Received GET request | get_unique_pet_names")
    try:
        full_file_path = "merged_data/merged_pet_data.parquet"
        # Read the Parquet file from storage
        df = await storage_client.read_parquet_data(file_name=full_file_path)

        if df is None or df.empty:
            logger.warning("File 'merged_pet_data.parquet' not found or empty.")
            raise HTTPException(status_code=404, detail="File 'merged_pet_data.parquet' not found or empty")

        if "name" not in df.columns:
            logger.error("Column 'name' not found in the DataFrame.")
            raise HTTPException(status_code=500, detail="Internal Server Error: 'name' column missing")

        # Apply the cleaning function to the 'name' column
        cleaned_names = df["name"].apply(clean_pet_name).dropna().unique().tolist()

        # Filter out any potential None values that might have slipped through (though dropna should handle it)
        unique_cleaned_names = [name for name in cleaned_names if name]

        logger.info(f"Found {len(unique_cleaned_names)} unique cleaned pet names.")
        return unique_cleaned_names

    except HTTPException:
        raise # Re-raise HTTPException to let FastAPI handle it
    except Exception as e:
        logger.exception("Failed to get unique pet names from 'merged_pet_data.parquet'")
        raise HTTPException(
            status_code=500,
            detail=f"Internal Server Error: {str(e)}"
        )


# --- get random set of pets from merged_pet_data
@router.get("/pets/merged_data/random")
async def get_random_pet_records(
    num_samples: int = 5,
    pet_type: str = None,
    age: int = None,
    gender: str = None,
    size: str = None,
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Return a random sample of pet records from the merged data file.
    - `num_samples`: Number of samples to return (default: 5)
    - `pet_type`: Filter by pet type
    - `age`: Filter by age
    - `gender`: Filter by gender
    - `size`: Filter by size
    """
    try:
        full_file_path = "merged_data/merged_pet_data.parquet"
        # Read the Parquet file from storage
        df = await storage_client.read_parquet_data(file_name=full_file_path)

        if df is None or df.empty:
            raise HTTPException(status_code=404, detail="File 'merged_pet_data.parquet' not found or empty")

        # Apply filters first
        filtered_df = df
        if pet_type:
            filtered_df = filtered_df[filtered_df["type"] == pet_type]
        if age is not None:
            filtered_df = filtered_df[filtered_df["age"] == age]
        if gender:
            filtered_df = filtered_df[filtered_df["gender"] == gender]
        if size:
            filtered_df = filtered_df[filtered_df["size"] == size]

        # Get random sample
        if len(filtered_df) >= num_samples:
            sample_data = filtered_df.sample(n=num_samples).to_dict('records')
        else:
            sample_data = filtered_df.to_dict('records')

        return {
            "success": True,
            "message": f"Returned {len(sample_data)} random records from file 'merged_pet_data.parquet'",
            "data": sample_data
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to get random pet records from 'merged_pet_data.parquet'")
        raise HTTPException(
            status_code=500,
            detail=f"Internal Server Error: {str(e)}"
        )
    

# --- generate game boards
@router.post("/pets/game_boards", response_model=Dict)
async def create_set_of_game_boards(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Create and save a file containing 10 game boards, each with 5 random pets.
    """
    try:
        result = await create_game_boards_parquet_file(storage_client)
        return result
    except Exception as e:
        logger.exception("Failed to process game boards request")
        raise HTTPException(status_code=500, detail=str(e))
    

# --- Read file by name in game_boards.parquet
@router.get("/pets/game_boards", response_model=ParquetReadDataResponse)
async def get_parquet_game_boards(
    pet_type: str = None,
    age: int = None,
    gender: str = None,
    size: str = None,
    # name: str = None,
    # num_samples: int = 5,
    storage_client: AzureDataStorageClient = Depends(get_storage),
):
    """
    Read and return contents of a specific Parquet file with optional filtering.
    - `num_samples`: Number of sample records to return (default: 5)
    - `pet_type`: Filter by pet type
    - `age`: Filter by age
    - `gender`: Filter by gender
    - `size`: Filter by size
    - `name`: Filter by name
    """
    try:
        full_file_path = "game_boards.parquet"
        # Read the Parquet file from storage
        df = await storage_client.read_parquet_data(full_file_path)

        if df is None or df.empty:
            raise HTTPException(status_code=404, detail="File 'game_boards.parquet' not found or empty")

        # Apply filters
        filtered_df = df
        if pet_type:
            filtered_df = filtered_df[filtered_df["type"] == pet_type]
        if age is not None:
            filtered_df = filtered_df[filtered_df["age"] == age]
        if gender:
            filtered_df = filtered_df[filtered_df["gender"] == gender]
        if size:
            filtered_df = filtered_df[filtered_df["size"] == size]
        # if name:
        #     # Clean the input name
        #     cleaned_name = clean_pet_name(name)

        #     if cleaned_name:
        #         filtered_df = filtered_df[filtered_df["name"].str.contains(cleaned_name, na=False)]
        
        # Clean names in the resulting DataFrame
        # filtered_df["cleaned_name"] = filtered_df.apply(lambda row: clean_pet_name(row["name"]), axis=1)

        # Convert numpy.ndarray to list for JSON serialization
        for col in filtered_df.select_dtypes([np.ndarray]).columns:
            filtered_df[col] = filtered_df[col].apply(list)

        # Convert DataFrame to list of dictionaries for JSON serialization
        data = filtered_df.to_dict('records')

        return {
            "success": True,
            "count": len(data),
            # "sample_data": data[:num_samples],  # Show first 5 records as sample
            "data": data
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to read parquet file 'game_boards.parquet'")
        raise HTTPException(
            status_code=500,
            detail=f"Internal Server Error: {str(e)}"
        )