#!/bin/bash
set -e

# Load environment variables from .env
if [ -f .env ]; then
  export $(cat .env | grep -v '#' | xargs)
fi

# Start the application
exec python -m uvicorn main:app --host 0.0.0.0 --port 8000 