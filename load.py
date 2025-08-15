"""
load.py

Module for database connection, table creation, and data loading operations 
in the ETL pipeline.

Responsibilities:
- Connect to PostgreSQL using environment variables (with defaults).
- Read CSV data into a pandas DataFrame.
- Create database tables from SQL schema files if they do not already exist.
- Load transformed booking data and currency rate data into PostgreSQL.

Environment Variables:
    DB_HOST (default: "localhost")
    DB_USER (default: "etl_user")
    DB_PASSWORD (default: "etl_pass")
    DB_NAME (default: "etl_db")
    DB_PORT (default: "5432")

Dependencies:
    - pandas
    - psycopg2
    - bookings.sql (schema for bookings table)
    - currency_rates.sql (schema for currency rates table)
    - config (TABLE_NAME)
"""

import os
import logging
import psycopg2
import pandas as pd
from config import TABLE_NAME

# Configure logging for database operations
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Database configuration from environment variables with defaults
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_USER = os.environ.get("DB_USER", "etl_user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "etl_pass")
DB_NAME = os.environ.get("DB_NAME", "etl_db")
DB_PORT = os.environ.get("DB_PORT", "5432")


def get_conn():
    """
    Create and return a new PostgreSQL database connection.

    Connection parameters are read from environment variables, 
    with fallback defaults if not set.

    Returns:
        psycopg2.extensions.connection: Active PostgreSQL connection object.
    """
    try:
        logging.info(f"Connecting to database '{DB_NAME}' as user '{DB_USER}' on host '{DB_HOST}'")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        raise


def read_csv_data():
    """
    Read currency rate data from `currency_rates.csv` into a DataFrame.

    Returns:
        pandas.DataFrame: Data from the CSV file.
    """
    try:
        csv_df = pd.read_csv("currency_rates.csv")
        logging.info(f"CSV data loaded. Shape: {csv_df.shape}")
        return csv_df
    except FileNotFoundError as e:
        logging.error(f"CSV file not found: {e}")
        raise
    except pd.errors.ParserError as e:
        logging.error(f"Error parsing CSV file: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error reading CSV: {e}")
        raise


def create_booking_table():
    """
    Create the `bookings` table in PostgreSQL if it does not exist.

    Reads and executes the SQL schema from `bookings.sql`.
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        with open(f"{TABLE_NAME}.sql", "r") as f:
            sql = f.read()
            cur.execute(sql)
        conn.commit()
        logging.info("Bookings table created or already exists.")
    except FileNotFoundError as e:
        logging.error(f"Bookings SQL file not found: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to create bookings table: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def create_rate_table():
    """
    Create the `currency_rates` table in PostgreSQL if it does not exist.

    Reads and executes the SQL schema from `currency_rates.sql`.
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        with open("currency_rates.sql", "r") as f:
            sql = f.read()
            cur.execute(sql)
        conn.commit()
        logging.info("Currency rates table created or already exists.")
    except FileNotFoundError as e:
        logging.error(f"Currency rates SQL file not found: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to create currency rates table: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def load_to_postgres(df):
    """
    Load transformed booking data into the `bookings` table.

    Args:
        df (pandas.DataFrame): DataFrame containing booking records.
                               Expected columns:
                               - booking_id
                               - check_in_date
                               - check_out_date
                               - owner_company
                               - owner_company_country

    Notes:
        Uses `ON CONFLICT (booking_id) DO NOTHING` to avoid inserting duplicates.
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO bookings (booking_id, check_in_date, check_out_date, owner_company, owner_company_country)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (booking_id) DO NOTHING;
            """, (
                row["booking_id"],
                row["check_in_date"],
                row["check_out_date"],
                row["owner_company"],
                row["owner_company_country"]
            ))
        conn.commit()
        logging.info(f"{len(df)} booking records loaded into PostgreSQL.")
    except KeyError as e:
        logging.error(f"Missing expected column in DataFrame: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to load booking data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def load_csv_to_postgres(csv_df):
    """
    Load currency rate data from a DataFrame into the `currency_rates` table.

    Args:
        csv_df (pandas.DataFrame): DataFrame containing currency rates.
                                   Expected columns:
                                   - from_currency
                                   - to_currency
                                   - rate
                                   - rate_date
    """
    try:
        conn = get_conn()
        cur = conn.cursor()
        for _, row in csv_df.iterrows():
            cur.execute("""
                INSERT INTO currency_rates (from_currency, to_currency, rate, rate_date)
                VALUES (%s, %s, %s, %s)
            """, (
                row["from_currency"],
                row["to_currency"],
                row["rate"],
                row["rate_date"]
            ))
        conn.commit()
        logging.info(f"{len(csv_df)} currency rate records loaded into PostgreSQL.")
    except KeyError as e:
        logging.error(f"Missing expected column in CSV DataFrame: {e}")
        raise
    except Exception as e:
        logging.error(f"Failed to load currency rate data: {e}")
        raise
    finally:
        cur.close()
        conn.close()
