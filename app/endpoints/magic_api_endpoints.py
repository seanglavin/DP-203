import httpx
import asyncio
import pandas as pd
import time
import math
import json
import numpy as np
import random
import re
from urllib.error import URLError
from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Optional, List, Dict, Any

from app.services.azure_storage_client import AzureDataStorageClient, get_mtg_storage_client
from app.services.scryfall import fetch_scryfall_cards_by_set, fetch_all_scryfall_sets
from app.config_settings import settings
from logger_config import logger


router = APIRouter()

# --- Configuration / Constants ---
SCRYFALL_SETS_FILE_PATH = "scryfall/all_sets.json"
MERGED_CORE_EXP_CARDS_PATH = "scryfall/cards/combined_all_core_expansion.json"
DAILY_SUBSET_FILE = "scryfall/cards/daily1000.json"
SUBSET_SIZE = 1000 # Number of cards to include in the daily subset
MIN_PRICE_FOR_SUBSET = 0.1
TARGET_SET_TYPES = ['core', 'expansion'] # Define target set types


# storage client
async def get_storage():
    """Dependency to get storage client"""
    return get_mtg_storage_client()


#
# -- Utils
#

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
    

async def generate_daily_subset(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Reads the main merged card file, filters for cards with a price greater
    than MIN_PRICE_FOR_SUBSET, selects a random subset, and uploads it
    to DAILY_SUBSET_FILE.
    """
    logger.info(f"Starting daily random card subset generation (size: {SUBSET_SIZE}, min_price: ${MIN_PRICE_FOR_SUBSET:.2f})...")

    try:
        # 1. Read the main merged file
        logger.info(f"Reading main merged file: {MERGED_CORE_EXP_CARDS_PATH}")
        all_cards_list = await storage_client.read_data(file_name=MERGED_CORE_EXP_CARDS_PATH, file_type="json")

        # ... (Error handling for reading file remains the same) ...
        if all_cards_list is None:
            logger.error(f"Source merged file not found or invalid: {MERGED_CORE_EXP_CARDS_PATH}. Cannot generate subset.")
            return
        if not isinstance(all_cards_list, list):
             logger.error(f"Expected a list from {MERGED_CORE_EXP_CARDS_PATH}, but got {type(all_cards_list)}. Cannot generate subset.")
             return
        if not all_cards_list:
             logger.warning(f"Source merged file is empty: {MERGED_CORE_EXP_CARDS_PATH}. Cannot generate subset.")
             return

        total_cards_read = len(all_cards_list)
        logger.info(f"Successfully read {total_cards_read} cards from {MERGED_CORE_EXP_CARDS_PATH}.")

        # --- Add Filtering Step ---
        logger.info(f"Filtering cards with price > ${MIN_PRICE_FOR_SUBSET:.2f}...")
        priced_cards_list = []
        for card in all_cards_list:
            try:
                # Safely access nested price
                price_str = card.get('prices', {}).get('usd')
                if price_str is not None:
                    price_float = float(price_str)
                    if price_float > MIN_PRICE_FOR_SUBSET:
                        priced_cards_list.append(card)
            except (ValueError, TypeError) as price_err:
                # Log if price conversion fails for a card, but continue
                card_id = card.get('id', 'N/A')
                logger.warning(f"Could not parse price for card ID {card_id} (price: '{price_str}'): {price_err}")
            except Exception as filter_err:
                 card_id = card.get('id', 'N/A')
                 logger.warning(f"Unexpected error filtering card ID {card_id}: {filter_err}")


        total_priced_cards = len(priced_cards_list)
        logger.info(f"Found {total_priced_cards} cards meeting the price filter criteria.")

        if total_priced_cards == 0:
            logger.warning(f"No cards found with price > ${MIN_PRICE_FOR_SUBSET:.2f}. Cannot generate subset.")
            # Optionally: Upload an empty file or handle differently
            # await storage_client.upload_json_data(data=[], file_name=DAILY_SUBSET_FILE)
            return
        # --- End Filtering Step ---

        # 2. Select the random subset *from the filtered list*
        actual_subset_size = min(SUBSET_SIZE, total_priced_cards) # Sample from available priced cards
        logger.info(f"Selecting a random subset of {actual_subset_size} cards from the filtered list...")

        if total_priced_cards <= SUBSET_SIZE:
            # If total priced cards is less than or equal to desired subset size, use all priced cards
            daily_subset = priced_cards_list
            logger.info(f"Total priced cards ({total_priced_cards}) are less than or equal to subset size ({SUBSET_SIZE}). Using all priced cards.")
        else:
            # Use random.sample on the filtered list
            daily_subset = random.sample(priced_cards_list, actual_subset_size)

        logger.info(f"Selected {len(daily_subset)} cards for the daily subset.")

        # 3. Upload the subset
        logger.info(f"Uploading daily subset to: {DAILY_SUBSET_FILE}")
        await storage_client.upload_json_data(data=daily_subset, file_name=DAILY_SUBSET_FILE)

        logger.info(f"Successfully generated and uploaded daily subset file: {DAILY_SUBSET_FILE}")

    except Exception as e:
        logger.exception(f"An error occurred during daily subset generation: {e}")



# --- Endpoints for Scryfall | Magic: The Gathering API ---


###
###
###
###
### POST sequence to get Scryfall data and save to storage, run once a week?


@router.post("/scryfall/sets")
async def save_scryfall_sets_to_storage(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Fetches all set data from the Scryfall API (/sets endpoint) and saves the
    raw list of set objects to Azure storage as a JSON file defined by
    SCRYFALL_SETS_FILE_PATH.
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


# --- COMBINED UPDATE ENDPOINT ---
@router.post("/scryfall/cards/update-core-expansion")
async def update_merged_core_expansion_cards(
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Updates the main merged card data file for core and expansion sets.

    Steps:
    1. Reads the list of all sets from SCRYFALL_SETS_FILE_PATH.
    2. Filters for sets matching TARGET_SET_TYPES ('core', 'expansion').
    3. Fetches card data for each target set directly from the Scryfall API.
    4. Combines all fetched cards into a single list in memory.
    5. Saves the combined list to MERGED_CORE_EXP_CARDS_PATH as a JSON file.
    6. Optionally triggers the daily subset generation upon successful completion.

    This endpoint replaces the previous separate fetch/partition and merge steps.
    """
    logger.info("Starting update process for merged core and expansion cards.")

    all_fetched_cards = [] # List to hold all cards in memory
    sets_processed_count = 0
    fetch_error_count = 0
    sets_with_no_cards = 0

    try:
        # 1. Read the Scryfall set list file (Prerequisite)
        logger.info(f"Reading set list from: {SCRYFALL_SETS_FILE_PATH}")
        set_data_list = await storage_client.read_data(file_name=SCRYFALL_SETS_FILE_PATH, file_type="json")

        if set_data_list is None:
            logger.error(f"Scryfall set list file not found or invalid: {SCRYFALL_SETS_FILE_PATH}. Cannot proceed.")
            raise HTTPException(status_code=404, detail=f"Scryfall set list file not found: {SCRYFALL_SETS_FILE_PATH}. Run POST /scryfall/sets first.")
        if not isinstance(set_data_list, list):
             logger.error(f"Expected a list from {SCRYFALL_SETS_FILE_PATH}, but got {type(set_data_list)}. Cannot proceed.")
             raise HTTPException(status_code=500, detail=f"Invalid format in set list file: {SCRYFALL_SETS_FILE_PATH}")

        logger.info(f"Read {len(set_data_list)} sets from the list file.")

        # 2. Filter for core and expansion sets
        target_sets_list = [
            set_dict for set_dict in set_data_list
            if set_dict.get('set_type') in TARGET_SET_TYPES # Use constant
        ]

        if not target_sets_list:
            logger.warning(f"No sets of types {TARGET_SET_TYPES} found in the set list. Nothing to update.")
            return {"message": f"No sets of types {TARGET_SET_TYPES} found. Merged file not updated."}

        total_sets_to_process = len(target_sets_list)
        logger.info(f"Found {total_sets_to_process} sets of type {TARGET_SET_TYPES} to process.")

        # 3. Iterate through target sets, fetch cards, and append to list
        for set_row in target_sets_list:
            current_set_code = set_row.get('code')
            current_set_name = set_row.get('name')
            sets_processed_count += 1

            if not all([current_set_code, current_set_name]):
                 logger.warning(f"Skipping set entry due to missing code or name: {set_row}")
                 continue

            logger.info(f"--- Processing set {sets_processed_count}/{total_sets_to_process}: {current_set_code} ({current_set_name}) ---")

            try:
                logger.debug(f"Fetching card data for set '{current_set_code}' from Scryfall API...")
                card_data_list = await fetch_scryfall_cards_by_set(current_set_code) # Assumes this helper exists and works

                if card_data_list is None:
                    # fetch_scryfall_cards_by_set should ideally raise an error or return []
                    # Handling None just in case
                    logger.error(f"Fetching card data for set {current_set_code} returned None. Skipping.")
                    fetch_error_count += 1
                    continue

                if not card_data_list:
                    logger.info(f"No cards found for set {current_set_code} from Scryfall.")
                    sets_with_no_cards += 1
                    continue

                logger.debug(f"Fetched {len(card_data_list)} cards for set '{current_set_code}'. Appending to main list.")
                all_fetched_cards.extend(card_data_list) # Append directly

            except Exception as fetch_err:
                # Log error from fetching a specific set but continue with others
                logger.exception(f"Error fetching cards for set '{current_set_code}': {fetch_err}")
                fetch_error_count += 1
                # Decide if you want to stop the whole process on a single set failure

        # 4. Save the combined list to the single merged file
        if not all_fetched_cards:
             logger.warning("No cards were fetched successfully across all sets. Skipping save of merged file.")
             return {"message": "Update process completed, but no cards were fetched or saved."}

        logger.info(f"Saving {len(all_fetched_cards)} combined cards to: {MERGED_CORE_EXP_CARDS_PATH}")
        try:
            # Use upload_json_data for consistency if available and appropriate
            await storage_client.upload_data(data=all_fetched_cards, file_name=MERGED_CORE_EXP_CARDS_PATH, file_type="json")
            logger.info(f"Successfully saved merged card file: {MERGED_CORE_EXP_CARDS_PATH}")

            # --- Optional: Trigger daily subset generation ---
            try:
                 logger.info("Triggering daily subset generation after successful merge...")
                 # Pass the already retrieved storage_client instance
                 await generate_daily_subset(storage_client=storage_client)
                 logger.info("Daily subset generation triggered.")
            except Exception as subset_err:
                 logger.exception(f"Error triggering daily subset generation after merge: {subset_err}")
                 # Log the error but don't fail the main update process
            # --- End Optional Trigger ---

        except Exception as upload_err:
            logger.exception(f"Error uploading combined file {MERGED_CORE_EXP_CARDS_PATH}: {upload_err}")
            raise HTTPException(status_code=500, detail=f"Failed to upload combined file: {upload_err}")

        # 5. Return summary
        summary_message = (
            f"Finished updating merged core/expansion cards. "
            f"Total sets processed: {sets_processed_count}. "
            f"Sets with fetch errors: {fetch_error_count}. "
            f"Sets with no cards found: {sets_with_no_cards}. "
            f"Total cards saved to {MERGED_CORE_EXP_CARDS_PATH}: {len(all_fetched_cards)}."
        )
        logger.info(summary_message)
        return {"message": summary_message}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.exception(f"Critical error during merged card update process: {e}")
        raise HTTPException(status_code=500, detail=f"Failed during merged card update: {str(e)}")


@router.post("/scryfall/cards/update-daily1000", status_code=202)
async def trigger_generate_daily_subset(storage_client: AzureDataStorageClient = Depends(get_storage)):
    """
    Manually triggers the generation of the daily random subset file (daily1000).

    This reads the main merged card file (MERGED_CORE_EXP_CARDS_PATH),
    filters for cards priced > MIN_PRICE_FOR_SUBSET, selects a random subset
    (size defined by SUBSET_SIZE), and saves it to DAILY_SUBSET_FILE.

    Note: This is also triggered automatically after a successful run of
    POST /scryfall/cards/update-core-expansion.
    """
    logger.info("Manual trigger received for daily random subset generation (daily1000).") # Updated log
    try:
        # Call the existing async function that contains the logic
        await generate_daily_subset(storage_client=storage_client)
        message = "Daily random subset generation process (daily1000) triggered successfully." # Updated message
        logger.info(message)
        # Return 202 Accepted, as the process might run for a bit
        return {"message": message}
    except Exception as e:
        # Catch any unexpected errors during the trigger/execution
        logger.exception(f"Error manually triggering daily subset generation (daily1000): {e}") # Updated log
        raise HTTPException(status_code=500, detail=f"Failed to trigger daily subset generation (daily1000): {str(e)}") # Updated detail


###
###
###
###
###


@router.get("/scryfall/sets", response_model=List[Dict])
async def get_scryfall_sets_from_storage(
    storage_client: AzureDataStorageClient = Depends(get_storage),
    code: Optional[str] = Query(None, description="Filter by set code (case-insensitive contains)"),
    name: Optional[str] = Query(None, description="Filter by set name (case-insensitive contains)"),
    set_type: Optional[str] = Query(None, description="Filter by exact set type (e.g., 'core', 'expansion')")
):
    """
    Reads the list of Scryfall set objects from the stored JSON file
    (SCRYFALL_SETS_FILE_PATH) in Azure storage, with optional filtering.
    """
    logger.info(f"Attempting to read Scryfall sets from storage: {SCRYFALL_SETS_FILE_PATH}")
    try:
        # 1. Read the JSON file into a list of dictionaries
        set_data_list = await storage_client.read_data(file_name=SCRYFALL_SETS_FILE_PATH, file_type="json")

        if set_data_list is None:
            logger.warning(f"Scryfall set file not found in storage: {SCRYFALL_SETS_FILE_PATH}")
            raise HTTPException(status_code=404, detail=f"Scryfall set data file not found at {SCRYFALL_SETS_FILE_PATH}. Run POST /scryfall/sets first.")

        if not isinstance(set_data_list, list):
             logger.error(f"Expected a list from {SCRYFALL_SETS_FILE_PATH}, but got {type(set_data_list)}. Data might be corrupted.")
             raise HTTPException(status_code=500, detail=f"Invalid format in set data file: {SCRYFALL_SETS_FILE_PATH}")

        if not set_data_list:
             logger.info(f"Set data file {SCRYFALL_SETS_FILE_PATH} is empty. Returning empty list.")
             return []

        # --- Convert list to DataFrame for filtering ---
        try:
            set_df = pd.DataFrame(set_data_list)
        except Exception as df_err:
            logger.exception(f"Failed to convert set data list to DataFrame: {df_err}")
            raise HTTPException(status_code=500, detail="Failed to process set data structure.")
        # --- End Conversion ---

        logger.info(f"Successfully read {len(set_df)} sets from {SCRYFALL_SETS_FILE_PATH}. Applying filters...")

        # 2. Apply Filters to the DataFrame
        filtered_df = set_df # Start with the full DataFrame

        # Check if columns exist before filtering to prevent KeyErrors
        if code and 'code' in filtered_df.columns:
            logger.debug(f"Filtering by code (contains, ignore case): {code}")
            # Ensure the column is treated as string before using .str accessor
            filtered_df = filtered_df[filtered_df['code'].astype(str).str.contains(code, case=False, na=False)]
        elif code:
             logger.warning("Filter 'code' ignored: Column 'code' not found in data.")

        if name and 'name' in filtered_df.columns:
            logger.debug(f"Filtering by name (contains, ignore case): {name}")
            filtered_df = filtered_df[filtered_df['name'].astype(str).str.contains(name, case=False, na=False)]
        elif name:
             logger.warning("Filter 'name' ignored: Column 'name' not found in data.")

        if set_type and 'set_type' in filtered_df.columns:
            logger.debug(f"Filtering by set_type (exact match, ignore case): {set_type}")
            # Ensure comparison is done correctly, handling potential NaN/None
            filtered_df = filtered_df[filtered_df['set_type'].astype(str).str.lower() == set_type.lower()]
        elif set_type:
             logger.warning("Filter 'set_type' ignored: Column 'set_type' not found in data.")


        logger.info(f"Filtering complete. Returning {len(filtered_df)} sets.")

        # 3. Convert the filtered DataFrame back to list of dictionaries
        # Fill NaN/NaT with None for JSON compatibility
        result_list = filtered_df.replace({np.nan: None}).to_dict(orient="records")

        # Optional: Apply numpy conversion if needed, although to_dict usually handles it
        # result_list = convert_numpy_to_list(result_list)

        return result_list

    except HTTPException as http_exc:
        raise http_exc

        logger.error(f"Filtering error: Problem accessing column '{e}'.")
        raise HTTPException(status_code=400, detail=f"Invalid filter or data structure issue involving column '{e}'.")
    except Exception as e:
        logger.exception(f"Error reading or filtering Scryfall sets from storage ({SCRYFALL_SETS_FILE_PATH}): {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read or filter Scryfall set data: {str(e)}")


    

@router.get("/scryfall/cards/core-expansion", response_model=List[Dict[str, Any]])
async def get_merged_core_expansion_cards(
    storage_client: AzureDataStorageClient = Depends(get_storage),
    # Optional: Add query parameters for filtering or pagination later if needed
    # name: Optional[str] = Query(None, description="Filter by card name (case-insensitive contains)"),
    # skip: int = Query(0, ge=0, description="Number of records to skip for pagination"),
    # limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return")
):
    """
    Reads and returns the full list of merged core and expansion card data
    from the combined JSON file (MERGED_CORE_EXP_CARDS_PATH).
    """
    merged_file_path = "scryfall/cards/combined_all_core_expansion.json"
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


@router.get("/scryfall/cards/random", response_model=Dict[str, Any])
async def get_random_merged_card(
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Reads the merged core and expansion card data from the combined JSON file
    (MERGED_CORE_EXP_CARDS_PATH) and returns a single randomly selected card object.
    """
    merged_file_path = "scryfall/cards/combined_all_core_expansion.json"
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


@router.get("/scryfall/cards/daily1000", response_model=List[Dict[str, Any]]) # Renamed path
async def get_daily_random_subset(
    storage_client: AzureDataStorageClient = Depends(get_storage)
):
    """
    Reads and returns the pre-generated daily random subset of cards (daily1000)
    from the JSON file defined by DAILY_SUBSET_FILE.
    This file contains cards priced > MIN_PRICE_FOR_SUBSET and is updated
    by the daily generation process.
    """
    logger.info(f"Attempting to read daily random subset file: {DAILY_SUBSET_FILE}") # Log uses constant

    try:
        # 1. Read the pre-generated subset JSON file
        daily_subset_list = await storage_client.read_data(file_name=DAILY_SUBSET_FILE, file_type="json")

        if daily_subset_list is None:
            logger.error(f"Daily random subset file not found or invalid: {DAILY_SUBSET_FILE}")
            raise HTTPException(status_code=404, detail=f"Daily random subset file not found at {DAILY_SUBSET_FILE}. The daily generation job may have failed or found no cards meeting criteria.") # Updated detail

        if not isinstance(daily_subset_list, list):
             logger.error(f"Expected a list from {DAILY_SUBSET_FILE}, but got {type(daily_subset_list)}.")
             raise HTTPException(status_code=500, detail=f"Invalid format in daily subset file: {DAILY_SUBSET_FILE}")

        logger.info(f"Successfully read {len(daily_subset_list)} cards from {DAILY_SUBSET_FILE}.")

        # 2. Return the list
        return daily_subset_list

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.exception(f"Error reading daily subset file {DAILY_SUBSET_FILE}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read daily random subset: {str(e)}")
    


