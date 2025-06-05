from fastapi import APIRouter, HTTPException, Depends, Body
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select, desc
import base64

from src.db.database import get_db
from src.db.schema.user import User
from src.db.schema.reviews import Review, StyleReview
from src.db.schema.style_data import StyleData

from src.llm.llm_process import summarize_review, fit_item

router = APIRouter(tags=["llm"])

def decode_token(token: str) -> int:
    """Decode the base64 token to get the user ID"""
    try:
        decoded_bytes = base64.b64decode(token)
        return int(decoded_bytes.decode('utf-8'))
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")


class SummarizeReviewRequest(BaseModel):
    token: str
    style_id: int

class FitItemRequest(BaseModel):
    token: str
    site_id: str
    style_data: StyleData

@router.post("/summarize", tags=["llm"])
async def summarize_reviews(request: SummarizeReviewRequest, db: Session = Depends(get_db)):
    try:
        # Decode token to get user ID
        user_id = decode_token(request.token)
        
        # Check if user exists and get user info
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            return {"success": False, "message": "User not found", "data": None}
        
        # Get user info
        user_info = user.data if user.data else {}
        
        # Get reviews for the style
        review_query = select(Review.text).join(
            StyleReview, StyleReview.review_id == Review.id
        ).where(
            StyleReview.style_id == request.style_id
        ).order_by(
            desc(Review.writed_at)
        ).limit(20)
        
        review_results = db.execute(review_query).fetchall()
        
        # Concatenate review texts
        review_texts = [result[0] for result in review_results if result[0]]
        
        if not review_texts:
            return {"success": False, "message": "No reviews found for this style", "data": None}
        
        concatenated_review_text = "\n\n".join(review_texts)
        
        # Call LLM for review summarization
        summary_result = await summarize_review(user_info, concatenated_review_text)
        
        return {
            "success": True,
            "message": None,
            "data": summary_result
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        }

@router.post("/fit-analysis", tags=["llm"])
async def analyze_fit(request: FitItemRequest, db: Session = Depends(get_db)):
    try:
        # Decode token to get user ID
        user_id = decode_token(request.token)
        
        # Check if user exists and get user info
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            return {"success": False, "message": "User not found", "data": None}
        
        # Get user info
        user_info = user.data if user.data else {}
        
        # Convert style data to dict
        style_data_dict = request.style_data.dict()
        
        # Call LLM for fit analysis
        fit_result = await fit_item(user_info, style_data_dict)
        
        return {
            "success": True,
            "message": None,
            "data": fit_result
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "message": str(e),
            "data": None
        }
