import httpx
import asyncio
import pandas as pd
import time # Import time for sleep
import math
import json
import numpy as np
import random
from urllib.error import URLError # Import URLError
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional, List, Dict, Any

from app.services.azure_storage_client import AzureDataStorageClient, get_mtg_storage_client
from app.services.scryfall import fetch_scryfall_cards_by_set, fetch_all_scryfall_sets
from app.config_settings import settings
from logger_config import logger


router = APIRouter()

# Use dependency injection for the storage client
async def get_storage():
    """Dependency to get storage client"""
    return get_mtg_storage_client()



# -- Utils

def flatten_card_dict(card: Dict[str, Any], sep: str = '_') -> Dict[str, Any]:
    """
    Flattens a nested dictionary representing a Scryfall card.
    Handles nested dicts and lists (converts lists to comma-separated strings).
    """
    flat_card = {}

    def _flatten(x: Any, name: str = ''):
        if isinstance(x, dict):
            for key in x:
                _flatten(x[key], name + key + sep)
        elif isinstance(x, list):
            # Strategy: Convert list items to string and join with comma
            # Filter out None values before joining
            str_list = [str(item) for item in x if item is not None]
            flat_card[name[:-len(sep)]] = ','.join(str_list)
        else:
            # Handle simple values (str, int, float, bool, None)
            flat_card[name[:-len(sep)]] = x

    _flatten(card)
    return flat_card


def flatten_card_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies flatten_card_dict to each row of a DataFrame containing card data.
    """
    # Convert DataFrame rows to list of dicts first
    records = df.to_dict(orient='records')
    # Flatten each dictionary
    flat_records = [flatten_card_dict(record) for record in records]
    # Convert back to DataFrame
    flat_df = pd.DataFrame(flat_records)
    return flat_df

def convert_numpy_to_list(data: Any) -> Any:
    """Recursively converts numpy arrays within data structures to Python lists."""
    if isinstance(data, dict):
        return {k: convert_numpy_to_list(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_numpy_to_list(item) for item in data]
    elif isinstance(data, np.ndarray):
        return data.tolist() # Convert numpy array to list
    # Handle basic numpy types often found in DataFrames
    elif isinstance(data, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32, np.int64, np.uint8,
                           np.uint16, np.uint32, np.uint64)):
        return int(data)
    elif isinstance(data, (np.float16, np.float32, np.float64)):
        # Handle potential NaN/Inf values if necessary, otherwise convert directly
        if np.isnan(data):
            return None # Or another representation like 'NaN' string
        elif np.isinf(data):
            return None # Or another representation like 'Infinity' string
        return float(data)
    elif isinstance(data, np.bool_):
        return bool(data)
    elif isinstance(data, np.void): # Handle numpy void type if it appears
        return None
    elif isinstance(data, np.datetime64):
        if pd.isna(data):
            return None
        try:
            timestamp = pd.Timestamp(data)
            return timestamp.isoformat()
        except Exception as dt_err:
             logger.warning(f"Could not convert numpy.datetime64 ({data}) to ISO string: {dt_err}. Falling back to str().")
             return str(data)
    else:
        return data
    

# --- Daily Random Subset Generation ---
# --- Configuration ---
SOURCE_MERGED_FILE = "scryfall/cards/merged_cards/combined_all_core_expansion.json"
DAILY_SUBSET_FILE = "scryfall/cards/merged_cards/daily100.json"
SUBSET_SIZE = 100 # Number of cards to include in the daily subset
# --- End Configuration ---

async def generate_daily_subset(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Reads the main merged card file, selects a random subset,
    and uploads it to a new file for daily use.
    """
    logger.info("Starting daily random card subset generation...")

    try:
        # 1. Read the main merged file
        logger.info(f"Reading main merged file: {SOURCE_MERGED_FILE}")
        all_cards_list = await storage_client.read_data(file_name=SOURCE_MERGED_FILE, file_type="json")

        if all_cards_list is None:
            logger.error(f"Source merged file not found or invalid: {SOURCE_MERGED_FILE}. Cannot generate subset.")
            return # Exit script

        if not isinstance(all_cards_list, list):
             logger.error(f"Expected a list from {SOURCE_MERGED_FILE}, but got {type(all_cards_list)}. Cannot generate subset.")
             return # Exit script

        if not all_cards_list:
             logger.warning(f"Source merged file is empty: {SOURCE_MERGED_FILE}. Cannot generate subset.")
             return # Exit script

        total_cards = len(all_cards_list)
        logger.info(f"Successfully read {total_cards} cards from {SOURCE_MERGED_FILE}.")

        # 2. Select the random subset
        actual_subset_size = min(SUBSET_SIZE, total_cards) # Don't try to sample more than available
        logger.info(f"Selecting a random subset of {actual_subset_size} cards...")

        if total_cards <= SUBSET_SIZE:
            # If total cards is less than or equal to desired subset size, just use all cards
            daily_subset = all_cards_list
            logger.info("Total cards are less than or equal to subset size. Using all cards.")
        else:
            # Use random.sample for efficient random selection without replacement
            daily_subset = random.sample(all_cards_list, actual_subset_size)

        logger.info(f"Selected {len(daily_subset)} cards for the daily subset.")

        # 3. Upload the subset
        logger.info(f"Uploading daily subset to: {DAILY_SUBSET_FILE}")
        await storage_client.upload_json_data(data=daily_subset, file_name=DAILY_SUBSET_FILE)

        logger.info(f"Successfully generated and uploaded daily subset file: {DAILY_SUBSET_FILE}")

    except Exception as e:
        logger.exception(f"An error occurred during daily subset generation: {e}")



# --- Endpoints for Scryfall | Magic: The Gathering API ---


SCRYFALL_SETS_FILE_PATH = "scryfall/all_sets.json"


@router.post("/scryfall/sets/save")
async def save_scryfall_sets_to_storage(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Fetches all set data from the Scryfall API and saves it to Azure storage
    as a Parquet file.
    """
    logger.info("Initiating Scryfall set data fetch and save process.")
    try:
        # 1. Fetch data from Scryfall
        set_data_list = await fetch_all_scryfall_sets()

        if set_data_list is None:
            logger.error("Failed to fetch set data from Scryfall.")
            raise HTTPException(status_code=502, detail="Failed to fetch set data from Scryfall API.")

        if not set_data_list:
            logger.warning("Scryfall returned an empty list of sets.")
            return {"message": "No sets found from Scryfall. Nothing uploaded."}

        # 3. Upload to Azure Storage
        logger.info(f"Uploading DataFrame to Azure Storage at {SCRYFALL_SETS_FILE_PATH}...")
        await storage_client.upload_data(data=set_data_list, file_name=SCRYFALL_SETS_FILE_PATH, file_type="json")

        message = f"Successfully fetched {len(set_data_list)} sets from Scryfall and uploaded to {SCRYFALL_SETS_FILE_PATH}"
        logger.info(message)
        return {"message": message}

    except HTTPException as http_exc:
        # Re-raise HTTPExceptions directly
        raise http_exc
    except Exception as e:
        logger.exception(f"Error during Scryfall set fetch or save: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save Scryfall set data: {str(e)}")


@router.get("/scryfall/sets", response_model=List[Dict])
async def get_scryfall_sets_from_storage(
    storage_client: AzureDataStorageClient = Depends(get_storage),
    code: Optional[str] = Query(None, description="Filter by set code (case-insensitive contains)"),
    name: Optional[str] = Query(None, description="Filter by set name (case-insensitive contains)"),
    # released_at: Optional[str] = Query(None, description="Filter by exact release date (YYYY-MM-DD)"),
    set_type: Optional[str] = Query(None, description="Filter by exact set type (e.g., 'core', 'expansion')")
):
    """
    Reads Scryfall set data from the stored Parquet file in Azure storage,
    with optional filtering.
    """
    logger.info(f"Attempting to read Scryfall sets from storage: {SCRYFALL_SETS_FILE_PATH}")
    try:
        # 1. Read the Parquet file
        set_data_df = await storage_client.read_data(file_name=SCRYFALL_SETS_FILE_PATH, file_type="json")

        if set_data_df is None:
            logger.warning(f"Scryfall set file not found in storage: {SCRYFALL_SETS_FILE_PATH}")
            raise HTTPException(status_code=404, detail=f"Scryfall set data file not found at {SCRYFALL_SETS_FILE_PATH}. Run POST /scryfall/sets/save first.")

        logger.info(f"Successfully read {len(set_data_df)} sets from {SCRYFALL_SETS_FILE_PATH}. Applying filters...")

        # set_df = pd.DataFrame(set_data) # Convert to DataFrame for filtering

        # 2. Apply Filters
        filtered_df = set_data_df.copy() # Start with a copy

        if code:
            logger.debug(f"Filtering by code (contains, ignore case): {code}")
            filtered_df = filtered_df[filtered_df['code'].str.contains(code, case=False, na=False)]
        if name:
            logger.debug(f"Filtering by name (contains, ignore case): {name}")
            filtered_df = filtered_df[filtered_df['name'].str.contains(name, case=False, na=False)]
        if set_type:
            logger.debug(f"Filtering by set_type (exact match, ignore case): {set_type}")
            filtered_df = filtered_df[filtered_df['set_type'].str.lower() == set_type.lower()]

        logger.info(f"Filtering complete. Returning {len(filtered_df)} sets.")

        # 3. Convert to list of dictionaries
        # Fill NaN values to avoid JSON serialization errors, convert to Python native types
        result_list = filtered_df.astype(object).where(pd.notnull(filtered_df), None).to_dict(orient="records")

        return result_list

    except HTTPException as http_exc:
        raise http_exc
    except FileNotFoundError: # More specific exception if read_parquet_data raises it
         logger.error(f"Scryfall set file not found: {SCRYFALL_SETS_FILE_PATH}")
         raise HTTPException(status_code=404, detail=f"Scryfall set data file not found: {SCRYFALL_SETS_FILE_PATH}")
    except KeyError as e:
        logger.error(f"Filtering error: Column '{e}' not found in the Parquet file.")
        raise HTTPException(status_code=400, detail=f"Invalid filter: Column '{e}' does not exist in set data.")
    except Exception as e:
        logger.exception(f"Error reading or filtering Scryfall sets from storage ({SCRYFALL_SETS_FILE_PATH}): {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read or filter Scryfall set data: {str(e)}")



@router.get("/scryfall/cards/{set_code}", response_model=List[Dict[str, Any]])
async def get_scryfall_cards_by_set_code(
    set_code: str,
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Reads card data for a specific set code by finding all files matching
    'scryfall/cards/{set_code}_*.json', merging them, and returning the result.
    """
    safe_set_code = set_code.lower()
    # Construct a prefix to find files like 'dsk_expansion.parquet', 'dsk_core.parquet', etc.
    # Adding the underscore makes the prefix more specific.
    file_prefix = f"scryfall/cards/{safe_set_code}_"
    logger.info(f"Attempting to read Scryfall cards for set code '{safe_set_code}' using prefix: '{file_prefix}*.json'")

    all_cards_list = []
    read_error_count = 0
    total_read_duration = 0.0

    try:
        # 1. List files matching the prefix
        matching_files = await storage_client.list_files(prefix=file_prefix)

        # Filter further to ensure they end with .json (list_files might not guarantee this)
        json_files_to_read = [
            name for name in matching_files if name.endswith(".json")
        ]

        if not json_files_to_read:
            logger.warning(f"No Parquet files found matching prefix '{file_prefix}' for set code '{safe_set_code}'.")
            raise HTTPException(status_code=404, detail=f"No card data files found for set code '{safe_set_code}' matching pattern '{file_prefix}*.json'.")

        logger.info(f"Found {len(json_files_to_read)} files matching prefix to read: {json_files_to_read}")

        # 2. Read each matching file
        for i, file_path in enumerate(json_files_to_read):
            logger.debug(f"Reading file {i+1}/{len(json_files_to_read)}: {file_path}")
            start_read_time = time.time()
            try:
                list_of_dicts = await storage_client.read_data(file_name=file_path, file_type="json")
                read_duration = time.time() - start_read_time
                total_read_duration += read_duration
                if list_of_dicts is not None and list_of_dicts:
                    all_cards_list.extend(list_of_dicts)
                    logger.debug(f"Successfully read {len(list_of_dicts)} records from {file_path} in {read_duration:.4f}s.")
                elif list_of_dicts is None:
                     logger.warning(f"Could not read file (returned None): {file_path}. Skipping.")
                     read_error_count += 1
                else: # df is empty
                     logger.info(f"File is empty: {file_path}. Skipping.")

            except Exception as read_err:
                read_duration = time.time() - start_read_time
                total_read_duration += read_duration
                logger.exception(f"Error reading file {file_path} after {read_duration:.4f}s: {read_err}. Skipping.")
                read_error_count += 1

        logger.info(f"Total Parquet file read duration: {total_read_duration:.4f} seconds for {len(json_files_to_read)} files.")

        # 3. Check if any data was actually read
        if not all_cards_list:
            logger.error(f"Failed to read data from any of the matching files for prefix '{file_prefix}'. Read errors: {read_error_count}.")
            # Decide on appropriate status code - 404 if files existed but couldn't be read/were empty? Or 500?
            raise HTTPException(status_code=404, detail=f"Could not read card data for set code '{safe_set_code}'. Files might be empty or corrupted.")

        # --- Select specific columns for trivia ---
        trivia_columns = [
            'name', 
            'mana_cost', 
            'cmc', 
            'type_line', 
            'oracle_text',
            'power', 
            'toughness', 
            'loyalty', 
            'defense', 
            'keywords', 
            'rarity', 
            'artist',
            'flavor_text', 
            'image_uris_normal', 
            'set', 
            'set_name', 
            'released_at', 
            'collector_number'
        ]

        logger.debug(f"Filtering {len(all_cards_list)} combined records to keep trivia columns...")
        filtered_list = []
        for card_dict in all_cards_list:
            filtered_card = {key: card_dict.get(key) for key in trivia_columns if key in card_dict}
            filtered_list.append(filtered_card)
        logger.debug(f"Filtering complete. Returning {len(filtered_list)} records.")

        return filtered_list

    except HTTPException as http_exc:
        raise http_exc

    except Exception as e:
        logger.exception(f"Error processing request for set code '{safe_set_code}' using prefix '{file_prefix}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to process card data request for set code '{safe_set_code}': {str(e)}")


@router.post("/scryfall/cards/partition-core-expansion")
async def fetch_partition_upload_core_expansion_cards(
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Fetches card data for 'core' and 'expansion' sets directly from Scryfall,
    flattens the data, and saves each set's data individually to 'scryfall/cards/'
    with the naming scheme {set_code}_{set_name}.json.
    """
    logger.info("Starting partitioning process for core and expansion set cards by fetching from Scryfall.")

    sets_processed_count = 0
    files_saved_count = 0
    fetch_error_count = 0
    # flatten_error_count = 0
    save_error_count = 0
    sets_with_no_cards = 0
    target_set_types = ['core', 'expansion']

    try:
        # 1. Read the Scryfall set list file
        set_data_list = await storage_client.read_data(file_name=SCRYFALL_SETS_FILE_PATH, file_type="json")
        if set_data_list is None:
            logger.error(f"Scryfall set list file not found or invalid: {SCRYFALL_SETS_FILE_PATH}. Cannot proceed.")
            raise HTTPException(status_code=404, detail=f"Scryfall set list file not found or invalid: {SCRYFALL_SETS_FILE_PATH}")
        if not isinstance(set_data_list, list):
             logger.error(f"Expected a list from {SCRYFALL_SETS_FILE_PATH}, but got {type(set_data_list)}. Cannot proceed.")
             raise HTTPException(status_code=500, detail=f"Invalid format in set list file: {SCRYFALL_SETS_FILE_PATH}")

        logger.info(f"Read {len(set_data_list)} sets from the list file.")

        # 2. Filter for core and expansion sets using list comprehension
        target_sets_list = [
            set_dict for set_dict in set_data_list
            if set_dict.get('set_type') in target_set_types
        ]

        if not target_sets_list:
            logger.warning("No 'core' or 'expansion' sets found in the set list. Nothing to partition.")
            return {"message": "No 'core' or 'expansion' sets found. No files partitioned."}
        
        total_sets_to_process = len(target_sets_list)
        logger.info(f"Found {total_sets_to_process} sets of type 'core' or 'expansion' to process.")

        # 3. Iterate through target sets (now a list of dicts)
        for set_row in target_sets_list:
            current_set_code = set_row.get('code')
            current_set_type = set_row.get('set_type')
            current_set_name = set_row.get('name')
            sets_processed_count += 1

            # Check if the set code is valid
            if not all([current_set_code, current_set_type, current_set_name]):
                 logger.warning(f"Skipping set entry due to missing data: {set_row}")
                 continue

            logger.info(f"--- Processing set {sets_processed_count}/{total_sets_to_process}: {current_set_code} ({current_set_name}) Type: {current_set_type} ---")

            # Sanitize set name for use in filename (replace slashes, etc.)
            safe_set_name = current_set_name.replace('/', '_').replace('\\', '_')
            destination_card_file = f"scryfall/cards/{current_set_code}_{safe_set_name}.json" # Use sanitized name

            try:
                # 4. Fetch cards for the current set from Scryfall API
                logger.debug(f"Fetching card data for set '{current_set_code}_{current_set_name}' from Scryfall API...")
                card_data_list = await fetch_scryfall_cards_by_set(current_set_code)

                if card_data_list is None:
                    logger.error(f"Failed to fetch card data for set {current_set_code} from Scryfall. Skipping.")
                    fetch_error_count += 1
                    continue # Move to the next set

                if not card_data_list:
                    logger.info(f"No cards found for set {current_set_code} from Scryfall.")
                    sets_with_no_cards += 1
                    continue # Move to the next set

                logger.debug(f"Fetched {len(card_data_list)} cards for set '{current_set_code}'.")

                logger.info(f"Saving raw card data for set '{current_set_code}_{current_set_name}' to: {destination_card_file}")
                await storage_client.upload_data(data=card_data_list, file_name=destination_card_file, file_type="json")

                files_saved_count += 1
                logger.debug(f"Successfully saved {destination_card_file}")

            except Exception as process_error:
                # Catch errors during fetch/flatten/save for a specific set
                logger.exception(f"Error processing set '{current_set_code}_{current_set_name}': Fetching, flattening, or writing to {destination_card_file}. Error: {process_error}")
                save_error_count += 1 # Count as a general save/process error here
                # Continue to the next set even if one fails

        # 8. Return summary
        summary_message = (
            f"Finished partitioning 'core' and 'expansion' sets by fetching from Scryfall. "
            f"Total sets considered: {total_sets_to_process}. "
            f"Successfully fetched and saved files: {files_saved_count}. "
            f"Sets with fetch errors: {fetch_error_count}. "
            # f"Sets with flatten errors: {flatten_error_count}. "
            f"Sets with save errors: {save_error_count}. "
            f"Sets with no cards found: {sets_with_no_cards}."
        )
        logger.info(summary_message)
        return {"message": summary_message}

    except HTTPException as http_exc:
        # Handle specific HTTP errors like 404 for the set list file
        raise http_exc
    except Exception as e:
        logger.exception(f"Critical error during card partitioning initiation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed during card partitioning: {str(e)}")
    


@router.post("/scryfall/cards/merge-core-expansion")
async def merge_core_expansion_card_files(
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Merges card files from 'scryfall/cards/' into a single combined file:
    'scryfall/cards/merged_cards/combined_core_expansion.json'.
    """
    source_directory = "scryfall/cards/"
    # core_pattern = "_core.json"
    # expansion_pattern = "_expansion.json"
    combined_file_path = f"{source_directory}merged_cards/combined_all_core_expansion.json"

    logger.info(f"Starting merge process for core/expansion card files in '{source_directory}'.")
    logger.info(f"Target combined file: '{combined_file_path}'.")

    files_merged_count = 0
    read_error_count = 0
    all_cards_list = []

    try:
        # 1. List all potential files in the source directory using the modified list_files
        # This now returns a list of strings (names)
        all_blob_names = await storage_client.list_files(prefix=source_directory) # Call the modified method
        if not all_blob_names:
            logger.warning(f"No files found in directory '{source_directory}'. Nothing to merge.")
            return {"message": f"No files found in {source_directory}. Merge not performed."}

        # Filter for core and expansion files, excluding the combined file itself
        files_to_merge = [
            name for name in all_blob_names # Iterate directly over names
               if name != combined_file_path
        ]

        if not files_to_merge:
            logger.warning(f"No files found in '{source_directory}'. Nothing to merge.")
            return {"message": f"No core or expansion files found to merge in {source_directory}."}

        total_files_to_process = len(files_to_merge)
        logger.info(f"Found {total_files_to_process} core/expansion files to merge.")

        # 2. Read each file and collect DataFrames (No change needed here)
        for i, file_path in enumerate(files_to_merge):
            logger.debug(f"Reading file {i+1}/{total_files_to_process}: {file_path}")
            try:
                list_of_dicts = await storage_client.read_data(file_name=file_path, file_type="json")
                if list_of_dicts is not None and list_of_dicts:
                    all_cards_list.extend(list_of_dicts)
                    files_merged_count += 1
                elif list_of_dicts is None:
                     logger.warning(f"Could not read file (returned None): {file_path}. Skipping.")
                     read_error_count += 1
                else: # df is empty
                     logger.info(f"File is empty: {file_path}. Skipping.")

            except Exception as read_err:
                logger.exception(f"Error reading file {file_path}: {read_err}. Skipping.")
                read_error_count += 1


        logger.info(f"Uploading combined data to {combined_file_path}...")
        try:
            await storage_client.upload_data(data=all_cards_list, file_name=combined_file_path, file_type="json")

            logger.info(f"Successfully uploaded combined file: {combined_file_path}")

        except Exception as upload_err:
            logger.exception(f"Error uploading combined file {combined_file_path}: {upload_err}")
            raise HTTPException(status_code=500, detail=f"Failed to upload combined file: {upload_err}")

        # 5. Return summary (No change needed here)
        summary_message = (
            f"Successfully merged card data. "
            f"Files successfully read and merged: {files_merged_count}. "
            f"Files skipped due to read errors: {read_error_count}. "
            f"Combined file saved to: {combined_file_path} with {len(all_cards_list)} total records."
        )
        logger.info(summary_message)
        return {"message": summary_message}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.exception(f"Critical error during card file merge process: {e}")
        raise HTTPException(status_code=500, detail=f"Failed during card file merge: {str(e)}")
    

@router.get("/scryfall/cards/merged_cards/core-expansion", response_model=List[Dict[str, Any]])
async def get_merged_core_expansion_cards(
    storage_client: AzureDataStorageClient = Depends(get_storage),
    # Optional: Add query parameters for filtering or pagination later if needed
    # name: Optional[str] = Query(None, description="Filter by card name (case-insensitive contains)"),
    # skip: int = Query(0, ge=0, description="Number of records to skip for pagination"),
    # limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """
    Reads the merged core and expansion card data from the combined JSON file
    'scryfall/cards/merged_cards/combined_all_core_expansion.json'.
    """
    merged_file_path = "scryfall/cards/merged_cards/combined_all_core_expansion.json"
    logger.info(f"Attempting to read merged core/expansion card data from: {merged_file_path}")

    try:
        # 1. Read the combined JSON file
        all_cards_list = await storage_client.read_data(file_name=merged_file_path, file_type="json")

        if all_cards_list is None:
            logger.error(f"Merged card file not found or invalid: {merged_file_path}")
            raise HTTPException(status_code=404, detail=f"Merged card data file not found at {merged_file_path}. Run the merge process first.")

        if not isinstance(all_cards_list, list):
             logger.error(f"Expected a list from {merged_file_path}, but got {type(all_cards_list)}.")
             raise HTTPException(status_code=500, detail=f"Invalid format in merged card file: {merged_file_path}")

        logger.info(f"Successfully read {len(all_cards_list)} cards from {merged_file_path}.")

        # --- Placeholder for Filtering/Pagination ---
        # If you uncomment the Query parameters above, add filtering/pagination logic here.
        # Example (Filtering by name):
        # if name:
        #     name_lower = name.lower()
        #     all_cards_list = [card for card in all_cards_list if name_lower in card.get('name','').lower()]
        #     logger.info(f"Filtered down to {len(all_cards_list)} cards matching name '{name}'.")

        # Example (Pagination):
        # paginated_list = all_cards_list[skip : skip + limit]
        # logger.info(f"Returning {len(paginated_list)} cards (skip={skip}, limit={limit}).")
        # return paginated_list
        # --- End Placeholder ---

        # Return the full list for now
        return all_cards_list

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.exception(f"Error reading merged card data from {merged_file_path}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read merged card data: {str(e)}")


@router.get("/scryfall/cards/merged_cards/random", response_model=Dict[str, Any])
async def get_random_merged_card(
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Reads the merged core and expansion card data from the combined JSON file
    and returns a single randomly selected card.
    """
    merged_file_path = "scryfall/cards/merged_cards/combined_all_core_expansion.json"
    logger.info(f"Attempting to read merged card data from: {merged_file_path} to select a random card.")

    try:
        # 1. Read the combined JSON file
        all_cards_list = await storage_client.read_data(file_name=merged_file_path, file_type="json")

        if all_cards_list is None:
            logger.error(f"Merged card file not found or invalid: {merged_file_path}")
            raise HTTPException(status_code=404, detail=f"Merged card data file not found at {merged_file_path}. Run the merge process first.")

        if not isinstance(all_cards_list, list):
             logger.error(f"Expected a list from {merged_file_path}, but got {type(all_cards_list)}.")
             raise HTTPException(status_code=500, detail=f"Invalid format in merged card file: {merged_file_path}")

        if not all_cards_list:
             logger.warning(f"Merged card file is empty: {merged_file_path}. Cannot select a random card.")
             raise HTTPException(status_code=404, detail="No cards available in the merged file.")

        logger.info(f"Successfully read {len(all_cards_list)} cards from {merged_file_path}.")

        # 2. Select a random card from the list
        random_card = random.choice(all_cards_list)
        logger.info(f"Randomly selected card ID: {random_card.get('id', 'N/A')}")

        # 3. Return the single random card
        return random_card

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.exception(f"Error reading or selecting random card from {merged_file_path}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get random card: {str(e)}")


@router.get("/scryfall/cards/merged_cards/daily100", response_model=List[Dict[str, Any]])
async def get_daily_random_subset(
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Reads and returns the pre-generated daily random subset of cards.
    This file should be updated daily by a background script.
    """
    logger.info(f"Attempting to read daily random subset file: {DAILY_SUBSET_FILE}")

    try:
        # 1. Read the pre-generated subset JSON file
        daily_subset_list = await storage_client.read_data(file_name=DAILY_SUBSET_FILE, file_type="json")

        if daily_subset_list is None:
            logger.error(f"Daily random subset file not found or invalid: {DAILY_SUBSET_FILE}")
            # Return 404 - the file *should* exist if the daily job ran.
            raise HTTPException(status_code=404, detail=f"Daily random subset file not found at {DAILY_SUBSET_FILE}. The daily generation job may have failed.")

        if not isinstance(daily_subset_list, list):
             logger.error(f"Expected a list from {DAILY_SUBSET_FILE}, but got {type(daily_subset_list)}.")
             # Return 500 - indicates a problem with the generated file format.
             raise HTTPException(status_code=500, detail=f"Invalid format in daily subset file: {DAILY_SUBSET_FILE}")

        logger.info(f"Successfully read {len(daily_subset_list)} cards from {DAILY_SUBSET_FILE}.")

        # 2. Return the list
        return daily_subset_list

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.exception(f"Error reading daily subset file {DAILY_SUBSET_FILE}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read daily random subset: {str(e)}")
    

@router.post("/scryfall/cards/merged_cards/daily100", status_code=202) # Use 202 Accepted
async def trigger_generate_daily_subset(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Manually triggers the generation of the daily random subset file.
    This reads the main merged file, selects a random subset, and saves it.
    Note: This might take some time depending on the size of the merged file.
    """
    logger.info("Manual trigger received for daily random subset generation.")
    try:
        # Call the existing async function that contains the logic
        await generate_daily_subset(storage_client=storage_client)
        message = "Daily random subset generation process triggered successfully."
        logger.info(message)
        # Return 202 Accepted, as the process might run for a bit
        return {"message": message}
    except Exception as e:
        # Catch any unexpected errors during the trigger/execution
        logger.exception(f"Error manually triggering daily subset generation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to trigger daily subset generation: {str(e)}")

