# Use an official Python runtime as a parent image
FROM python:3.13-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# Using --no-cache-dir to reduce image size
RUN pip install --no-cache-dir -r requirements.txt

# Install curl for health checks
RUN apt-get update && apt-get install -y curl --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Copy the rest of the application code into the container
COPY . .

# Make port 8000 available to the world outside this container (Gunicorn will bind to this)
EXPOSE 8000

# Define environment variable for the database path inside the container
# This path will be used by config.py
ENV DATABASE_PATH=/app/database/data.db
ENV FLASK_APP=webapp.py
# For APScheduler logging to be visible via Docker logs
ENV PYTHONUNBUFFERED=1


# Create the data directory where the SQLite DB will be stored
# This directory will be a good candidate for a Docker volume
RUN mkdir -p /app/data
RUN mkdir -p /app/database

# Command to run the application using Gunicorn
# --workers 1: Start with one worker. Adjust based on your needs and server resources.
# --preload: Loads the application before forking worker processes.
#            This is important for our initial data import and scheduler setup
#            to run once in the master process.
# webapp:app: Tells Gunicorn to look for an object named 'app' in the 'webapp.py' module.

# Health check to ensure the application is responsive
HEALTHCHECK --interval=30s --timeout=5s --start-period=600s --retries=3 \
  CMD curl -f http://localhost:8000/ || exit 1

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--preload", "webapp:app"]
