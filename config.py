"""
Configuration constants for the ETL pipeline.

API_URL : str
    The base URL of the bookings API to extract data from.

PER_PAGE : int
    Number of records to fetch per page when paginating API requests.

TABLE_NAME : str
    Default name of the bookings table in the database.
"""

API_URL = "http://localhost:5000/api/bookings"
PER_PAGE = 50
TABLE_NAME = "bookings"
