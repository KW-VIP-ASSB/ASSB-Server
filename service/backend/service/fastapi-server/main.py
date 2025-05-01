from fastapi import FastAPI
from src.api.routes import router as api_router
from src.llm.routes import router as llm_router
from src.db.database import SessionLocal
from sqlalchemy import text
import logging

# Configure simple logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

app = FastAPI(title="API and LLM Service")

# API routes for database operations
app.include_router(api_router, prefix="/api", tags=["api"])

# LLM routes for analysis
app.include_router(llm_router, prefix="/llm", tags=["llm"])

@app.get("/")
async def root():
    """Root endpoint that checks database connection"""
    try:
        db = SessionLocal()
        # Verify connection is working with proper text() wrapper
        db.execute(text("SELECT 1"))
        db.close()
        return {"message": "Server is running", "db_status": "connected"}
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Database connection error: {error_msg}")
        return {"message": "Server is running", "db_status": "error", "error": error_msg}
