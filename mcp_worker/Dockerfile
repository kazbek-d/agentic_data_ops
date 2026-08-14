# Use the official stable and lightweight Python image based on Debian Bookworm
FROM python:3.11-slim-bookworm

# Configure environment variables so that Python does not buffer logs 
# and does not generate garbage .pyc cache files
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# STRICT ENGINEERING: Force the system locale to UTF-8 at the Linux level.
# No more PowerShell can impose a crooked encoding on us!
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Set the working directory inside the container
WORKDIR /app

# Copy only the dependency file first (for efficient caching of Docker layers)
COPY requirements.txt .

# Upgrade pip and install packages without storing the local wheel cache,
# to minimize the size of the final container
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy all the rest of the worker code (server.py, core/, data/) into the container
COPY . .

# Our HTTP server of "arms" will listen on port 5011
EXPOSE 5011

# Run our tool server
CMD ["python", "server.py"]