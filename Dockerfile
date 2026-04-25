FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for psutil and docker SDK
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire codebase
COPY . .

# Ensure PYTHONPATH includes the current directory
ENV PYTHONPATH=/app

# Default to Master, but can be overridden in K8s
CMD ["python3", "-m", "master.master"]
