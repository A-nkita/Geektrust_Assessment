# 1. Base image
FROM python:3.10-slim

# 2. Set working directory inside container
WORKDIR /app

# 3. Copy requirements first (layer caching)
COPY requirements.txt .

# 4. Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy project files
COPY scripts/ ./scripts
COPY sql/ ./sql

# 6. Default command (can be overridden)
CMD ["python", "scripts/ingestion.py"]
