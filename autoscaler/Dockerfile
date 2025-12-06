FROM python:3.11-slim

# Install Docker CLI (needed for Docker API)
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Expose ports
EXPOSE 8080 9090

# Run the application
CMD ["python", "-m", "app.main"]

