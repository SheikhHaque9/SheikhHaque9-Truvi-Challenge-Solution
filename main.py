"""
main.py

Entry point for the ETL (Extract, Transform, Load) pipeline.

This script orchestrates the end-to-end ETL process:
1. **Extract** — Fetches raw booking data from the API.
2. **Transform** — Cleans and standardizes the booking data.
3. **Load** — Creates database tables (if not existing) and loads transformed data
              along with currency rate data into PostgreSQL.
4. **Post-processing** — Creates the `country_currency` mapping table and the final
                         aggregated table.
5. **Presentation** — Displays the final processed table in a readable format.

Logging:
    Uses Python's built-in `logging` module for step-by-step status reporting.

Dependencies:
    - extract.extract_all_pages
    - transform.transform_data
    - load.read_csv_data
    - load.create_booking_table
    - load.create_rate_table
    - load.load_to_postgres
    - load.load_csv_to_postgres
    - create_country_currency.create_country_currency
    - create_final_table.create_final_table
    - present_final_table.present_final_table
"""

import logging
from extract import extract_all_pages
from transform import transform_data
from load import (
    read_csv_data,
    create_booking_table,
    create_rate_table,
    load_to_postgres,
    load_csv_to_postgres
)
from create_country_currency import create_country_currency
from create_final_table import create_final_table
from present_final_table import present_final_table

# Configure logging for the ETL process
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    """
    Execute the full ETL pipeline.

    Steps:
        1. Extract booking data from the API.
        2. Transform the extracted data.
        3. Create database tables if they do not exist.
        4. Load transformed booking data and currency rate data into PostgreSQL.
        5. Create the `country_currency` mapping table.
        6. Create the final processed table.
        7. Present the final table in a formatted output.

    Returns:
        None
    """
    logging.info("Starting ETL pipeline...")

    # Extract phase
    try:
        logging.info("Extracting data from API...")
        records = extract_all_pages()
        logging.info(f"Extracted {len(records)} records.")
        if not records:
            logging.warning("No records were extracted from API.")
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        return

    # Transform phase
    try:
        logging.info("Transforming data...")
        df = transform_data(records)
        logging.info(f"Data transformed. Shape: {df.shape}")
    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        return

    # Load phase
    try:
        logging.info("Creating database tables if needed...")
        create_booking_table()
        currency_rates = read_csv_data()
        create_rate_table()
    except Exception as e:
        logging.error(f"Table creation failed: {e}")
        return

    try:
        logging.info("Loading data into PostgreSQL...")
        load_to_postgres(df)
        load_csv_to_postgres(currency_rates)
        logging.info("Data loaded successfully.")
    except Exception as e:
        logging.error(f"Data loading failed: {e}")
        return

    # Post-processing
    try:
        logging.info("Creating country_currency table...")
        create_country_currency()
        logging.info("country_currency table created.")
    except Exception as e:
        logging.error(f"Failed to create country_currency table: {e}")

    try:
        logging.info("Creating the final table...")
        create_final_table()
        logging.info("Final table created.")
    except Exception as e:
        logging.error(f"Failed to create final table: {e}")

    # Presentation
    try:
        logging.info("Presenting the final table...")
        present_final_table(csv_path="output/final_table.csv") # Save CSV as 'final_table.csv' in the current working directory
        logging.info("Final table presented and CSV saved as 'final_table.csv'.")
    except Exception as e:
        logging.error(f"Presentation failed: {e}")

    logging.info("ETL pipeline finished.")

if __name__ == "__main__":
    main()
