"""
extract.py

Module responsible for extracting raw booking data from the API.

Functions:
    - extract_all_pages: Fetch all booking records from the API, handling pagination if necessary.

Logging:
    Uses Python's built-in `logging` module to report extraction status and errors.
"""

import requests
import math
import logging
from config import API_URL, PER_PAGE

# Configure logging for the extraction process
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_all_pages():
    """
    Fetch all booking records from the API, handling pagination if necessary.

    Returns:
        records (list): A list of dictionaries representing booking records.

    Raises:
        requests.exceptions.RequestException: If the API request fails.
        ValueError: If the response is not valid JSON or data is missing.
    """
    try:
        logging.info("Fetching first page from API...")
        first_page = requests.get(f"{API_URL}?page=1&per_page={PER_PAGE}")
        first_page.raise_for_status()
        first_page_data = first_page.json()

        if "results" not in first_page_data:
            raise ValueError("'results' key not found in API response")

        total_records = first_page_data.get("total")
        if total_records is None:
            raise ValueError("'total' key not found in API response")

        total_pages = math.ceil(total_records / PER_PAGE)
        records = first_page_data["results"]
        logging.info(f"Page 1 fetched, {len(records)} records added. Total pages: {total_pages}")

        # Fetch remaining pages
        for page in range(2, total_pages + 1):
            logging.info(f"Fetching page {page} from API...")
            resp = requests.get(f"{API_URL}?page={page}&per_page={PER_PAGE}")
            resp.raise_for_status()
            data = resp.json()

            if "results" not in data:
                raise ValueError(f"'results' key not found in API response for page {page}")

            records.extend(data["results"])
            logging.info(f"Page {page} fetched, {len(data['results'])} records added.")

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        raise
    except ValueError as e:
        logging.error(f"Failed to parse API response: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during extraction: {e}")
        raise

    logging.info(f"Extraction complete. Total records fetched: {len(records)}")
    return records
