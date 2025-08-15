"""
present_final_table.py

Module for retrieving and displaying the final processed table from the database.

This script:
- Establishes a database connection
- Executes a SQL query from an external file (`final_table_presentation.sql`)
- Loads the result into a pandas DataFrame
- Prints the table in a formatted style using the tabulate library
- Optionally exports the table to a CSV file

Environment Variables: 
    DB_HOST (default: "localhost") 
    DB_USER (default: "etl_user") 
    DB_PASSWORD (default: "etl_pass") 
    DB_NAME (default: "etl_db") 
    DB_PORT (default: "5432")

Dependencies:
    pandas
    tabulate
    load.get_conn (custom function for database connection)
    final_table_presentation.sql (SQL query file)
"""

import pandas as pd
from tabulate import tabulate
from load import get_conn
import logging

def present_final_table(csv_path=None):
    """
    Retrieve and display the final processed table in a readable format.
    Optionally exports the table to a CSV file if `csv_path` is provided.

    Steps:
        1. Connects to the database using `get_conn`.
        2. Reads the SQL query from `final_table_presentation.sql`.
        3. Executes the query and loads results into a DataFrame.
        4. Prints the DataFrame as a formatted table (PostgreSQL style).
        5. If `csv_path` is provided, saves the DataFrame to a CSV file.

    Args:
        csv_path (str, optional): Path to save the CSV file. Defaults to None.

    Returns:
        None
    """
    conn = None
    try:
        # Establish a database connection
        conn = get_conn()

        # Read the SQL query from the external file
        with open("final_table_presentation.sql") as f:
            query = f.read()

        # Execute the query and load results into a DataFrame
        df = pd.read_sql_query(query, conn)

        # Print the DataFrame as a PostgreSQL-style table without index
        print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))

        # Export to CSV if a path is provided
        if csv_path:
            try:
                df.to_csv(csv_path, index=False)
                logging.info(f"Final table exported to CSV: {csv_path}")
            except Exception as e:
                logging.error(f"Failed to export final table to CSV: {e}")

    except Exception as e:
        logging.error(f"Failed to retrieve or present final table: {e}")

    finally:
        # Close the database connection if it was opened
        if conn:
            conn.close()
