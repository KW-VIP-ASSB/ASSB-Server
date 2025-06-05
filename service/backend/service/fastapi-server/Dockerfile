FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libc-dev \
    python3-dev \
    libpq-dev \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
# Install dependencies including FastAPI, PostgreSQL client, LLM libraries and Langchain framework
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make start script executable
RUN chmod +x start.sh

# Set Python path to make imports work correctly
ENV PYTHONPATH=/app

# Set PostgreSQL connection environment variables
# host.docker.internal points to the host machine from within the container
ENV POSTGRES_HOST=219.255.242.174
ENV POSTGRES_USER=myuser
ENV POSTGRES_PASSWORD=mypassword
ENV POSTGRES_DB=mydatabase
ENV POSTGRES_PORT=5432

# Environment variables will be loaded from .env file

# Expose port
EXPOSE 8000

# Command to run the application with .env file
CMD ["./start.sh"]
