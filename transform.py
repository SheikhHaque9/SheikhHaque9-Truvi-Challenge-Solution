"""
transform.py

Module for transforming and cleaning bookings data.

This script converts raw booking records into a cleaned and standardized
DataFrame that is ready for analysis or storage. It handles:
- Removing invalid or duplicate booking IDs
- Parsing and validating date fields
- Standardizing text fields (company names and countries)
- Ensuring logical date order (check-out date is after check-in date)

Dependencies:
    pandas
"""

import pandas as pd
import logging

# Configure logging for the transformation process
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def transform_data(records):
    """
    Clean and prepare the bookings data for further processing.

    Args:
        records (list[dict]): A list of booking records, where each record
                              contains fields such as booking_id,
                              check_in_date, check_out_date, owner_company,
                              and owner_company_country.

    Returns:
        pd.DataFrame: A cleaned DataFrame with:
                      - Unique, non-null booking IDs
                      - Valid parsed dates
                      - Standardized text fields
                      - Logical date ordering
    """
    try:
        df = pd.DataFrame(records)
        logging.info(f"Initial data loaded. Shape: {df.shape}")

        # Remove records with missing booking_id and keep only the latest
        df = df.dropna(subset=["booking_id"])
        df = df.drop_duplicates(subset=["booking_id"], keep="last")
        logging.info(f"After dropping null/duplicate booking IDs. Shape: {df.shape}")

        # Convert check-in and check-out date strings to datetime objects
        # Coerce invalid formats to NaT (Not a Time)
        df["check_in_date"] = pd.to_datetime(df["check_in_date"], errors="coerce")
        df["check_out_date"] = pd.to_datetime(df["check_out_date"], errors="coerce")
        logging.info("Dates parsed successfully.")

        # Standardize text fields: remove surrounding spaces, title-case text
        df["owner_company"] = df["owner_company"].astype(str).str.strip().str.title()
        df["owner_company_country"] = df["owner_company_country"].astype(str).str.strip().str.title()
        logging.info("Text fields standardized.")

        # Keep only records where check-out date is after or same as check-in date
        df = df[df["check_out_date"] >= df["check_in_date"]]
        logging.info(f"After filtering invalid date ranges. Shape: {df.shape}")

        return df

    except KeyError as e:
        logging.error(f"Missing expected column in data: {e}")
        raise
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise