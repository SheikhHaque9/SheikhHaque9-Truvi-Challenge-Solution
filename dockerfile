# Dockerfile for the ETL project
# This Dockerfile builds an image that:
# 1. Sets up a Python 3.11 environment.
# 2. Installs project dependencies from requirements.txt.
# 3. Sets up a fake API for local testing (fake_api).
# 4. Copies the full ETL project into the container.
# 5. Exposes port 5000 for the fake API.
# 6. Starts the fake API in the background, then runs main.py.

FROM python:3.11-slim

WORKDIR /app

# Copy main requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy fake_api folder and install its requirements
COPY fake_api/requirements.txt ./fake_api/requirements.txt
COPY fake_api/fake_bookings.csv ./fake_api/fake_bookings.csv
COPY fake_api/fake_api.py ./fake_api/fake_api.py


WORKDIR /app/fake_api
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project
WORKDIR /app
COPY . .

# Expose the fake API port
EXPOSE 5000

# Start the fake API in the background, then run main.py
CMD ["sh", "-c", "cd /app/fake_api && python fake_api.py & sleep 2 && python /app/main.py"]

