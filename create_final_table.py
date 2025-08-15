"""
create_final_table.py

Module for creating the final consolidated table in PostgreSQL for the ETL pipeline.

Responsibilities:
- Connect to the PostgreSQL database.
- Execute the SQL schema in `final_table.sql` to create the final table.
- Commit the transaction and close the database connection.

Environment Variables:
    DB_HOST (default: "localhost")
    DB_USER (default: "etl_user")
    DB_PASSWORD (default: "etl_pass")
    DB_NAME (default: "etl_db")
    DB_PORT (default: "5432")

Dependencies:
    - load.py (get_conn function)
    - final_table.sql (SQL schema for the final table)
    - logging
"""

from load import get_conn
import logging

# Configure logging for table creation
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_final_table():
    """
    Create the final consolidated table in PostgreSQL.

    Process:
        1. Connect to the database using `get_conn`.
        2. Read the SQL schema from `final_table.sql`.
        3. Execute the SQL to create the table.
        4. Commit the transaction.
        5. Close the database connection.

    Notes:
        - Assumes that the SQL file `final_table.sql` contains the complete 
          CREATE TABLE statement for the final table.
        - Logs each major step for transparency in ETL execution.
    """
    conn = None
    cur = None
    try:
        # Connect to PostgreSQL
        conn = get_conn()
        cur = conn.cursor()
        logging.info("Connected to database. Creating final table...")

        # Execute SQL schema to create the table
        try:
            with open("final_table.sql", "r") as f:
                sql = f.read()
                cur.execute(sql)
            logging.info("SQL executed successfully.")
        except FileNotFoundError as e:
            logging.error(f"SQL file not found: {e}")
            raise
        except Exception as e:
            logging.error(f"Error executing SQL: {e}")
            raise

        # Commit transaction
        conn.commit()
        logging.info("Transaction committed. Final table creation complete.")

    except Exception as e:
        logging.error(f"Failed to create final table: {e}")
        if conn:
            conn.rollback()
            logging.info("Transaction rolled back due to error.")
        raise

    finally:
        # Close connection
        if cur:
            cur.close()
        if conn:
            conn.close()
        logging.info("Database connection closed.")
