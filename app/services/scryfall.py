import httpx
import asyncio
import time
from typing import List, Dict, Any, Optional
from logger_config import logger

SCRYFALL_API_BASE = "https://api.scryfall.com"
SCRYFALL_REQUEST_DELAY_SECONDS = 0.1


# -- Fetch Scryfall sets
async def fetch_all_scryfall_sets() -> Optional[List[Dict[str, Any]]]:
    """
    Fetches all set data from the Scryfall API (/sets endpoint).

    Returns:
        A list of dictionaries, where each dictionary represents a set,
        or None if an error occurs.
    """
    sets_url = f"{SCRYFALL_API_BASE}/sets"
    logger.info(f"Attempting to fetch all sets from Scryfall: {sets_url}")

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(sets_url)
            response.raise_for_status()  # Raise HTTPStatusError for bad responses (4xx or 5xx)

            data = response.json()

            if data and data.get("object") == "list" and "data" in data:
                set_list = data["data"]
                logger.info(f"Successfully fetched {len(set_list)} sets from Scryfall.")
                return set_list
            else:
                logger.warning(f"Scryfall response format unexpected or empty: {data}")
                return None

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching Scryfall sets: {e.response.status_code} - {e.response.text}")
            return None
        except httpx.RequestError as e:
            logger.error(f"Request error fetching Scryfall sets: {e}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected error fetching Scryfall sets: {e}")
            return None


# -- Fetch and save Scryfall cards by set code
async def fetch_scryfall_cards_by_set(set_code: str) -> Optional[List[Dict[str, Any]]]:
    """
    Fetches all card data for a given set code from the Scryfall API,
    handling pagination.

    Args:
        set_code: The Scryfall set code (e.g., 'mh3', 'woe').

    Returns:
        A list of dictionaries, where each dictionary represents a card,
        or None if an error occurs during fetching.
    """
    all_cards: List[Dict[str, Any]] = []
    # Construct the initial search URL. Using unique=cards and ordering by set number is common.
    # Adjust parameters as needed (e.g., include extras, change order).
    search_url: Optional[str] = f"{SCRYFALL_API_BASE}/cards/search?q=set%3A{set_code}&unique=cards&order=set"
    page_num = 1

    logger.info(f"Starting fetch for all cards in set '{set_code}' from Scryfall.")

    async with httpx.AsyncClient(timeout=60.0) as client: # Increased timeout for potentially many pages
        while search_url:
            try:
                logger.debug(f"Fetching page {page_num} for set '{set_code}' from URL: {search_url}")
                response = await client.get(search_url)
                response.raise_for_status()
                page_data = response.json()

                cards_in_page = page_data.get("data", [])
                if cards_in_page:
                    all_cards.extend(cards_in_page)
                    logger.debug(f"Fetched {len(cards_in_page)} cards from page {page_num}. Total so far: {len(all_cards)}")
                else:
                    logger.warning(f"No card data found in 'data' field on page {page_num} for set {set_code}. URL: {search_url}")

                # Check for the next page URL
                if page_data.get("has_more"):
                    search_url = page_data.get("next_page")
                    if not search_url:
                        logger.warning(f"Scryfall indicated 'has_more' but no 'next_page' URL provided for set {set_code} on page {page_num}.")
                        break # Stop if URL is missing
                    page_num += 1
                    # Respect Scryfall's polite request rate
                    await asyncio.sleep(SCRYFALL_REQUEST_DELAY_SECONDS)
                else:
                    search_url = None # No more pages

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error fetching page {page_num} for set {set_code}: {e.response.status_code} - {e.response.text}")
                return None # Indicate failure by returning None
            except httpx.RequestError as e:
                logger.error(f"Request error fetching page {page_num} for set {set_code}: {e}")
                return None # Indicate failure
            except Exception as e:
                logger.exception(f"Unexpected error fetching page {page_num} for set {set_code}: {e}")
                return None # Indicate failure

    logger.info(f"Finished fetching for set '{set_code}'. Total cards retrieved: {len(all_cards)}")
    return all_cards