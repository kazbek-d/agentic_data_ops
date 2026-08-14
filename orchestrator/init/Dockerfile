# Use ultra-lightweight and secure Python image for analytical scripts
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Disable buffering of Python logs for instant log streaming into Docker
ENV PYTHONUNBUFFERED=1

# Copy dependency file
COPY requirements.txt .

# Install libraries without caching to reduce container size
RUN pip install --no-cache-dir -r requirements.txt

# Copy indexing script itself
COPY rag_indexer.py .

# Entry point runs indexing only
CMD ["python", "rag_indexer.py"]