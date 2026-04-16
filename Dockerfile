# Use a slim Python 3.12 image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Set the Python path to include the current directory
ENV PYTHONPATH=/app

# Default command (can be overridden in docker-compose)
CMD ["python3", "-m", "master.master"]
