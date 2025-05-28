from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from src.db.database import get_db
from src.db.schema.user import User, Basket
from src.db.schema.style_data import StyleData
import base64
from pydantic import BaseModel

router = APIRouter(tags=["baskets"])

def decode_token(token: str) -> int:
    """Decode the base64 token to get the user ID"""
    try:
        decoded_bytes = base64.b64decode(token)
        return int(decoded_bytes.decode('utf-8'))
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

@router.get("/baskets/")
async def read_baskets(
    token: str,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    try:
        user_id = decode_token(token)
        
        # Check if user exists
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            return {
                "success": False,
                "message": "User not found",
                "data": None
            }
        
        baskets = db.query(Basket).filter(Basket.user_id == user_id).offset(skip).limit(limit).all()
        
        basket_list = []
        for basket in baskets:
            basket_dict = basket.__dict__
            basket_dict.pop("user_id")
            if "_sa_instance_state" in basket_dict:
                basket_dict.pop("_sa_instance_state")
            basket_list.append(basket_dict)
        
        return {
            "success": True,
            "message": None,
            "data": basket_list
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        }

@router.get("/baskets/{name}")
async def read_basket(name: str, token: str, db: Session = Depends(get_db)):
    try:
        user_id = decode_token(token)
        
        # Check if user exists
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            return {
                "success": False,
                "message": "User not found",
                "data": None
            }
        
        basket = db.query(Basket).filter(Basket.user_id == user_id, Basket.name == name).first()
        if basket is None:
            return {
                "success": False,
                "message": "Basket not found",
                "data": None
            }
        
        basket_dict = basket.__dict__
        basket_dict.pop("user_id")
        if "_sa_instance_state" in basket_dict:
            basket_dict.pop("_sa_instance_state")
        
        return {
            "success": True,
            "message": None,
            "data": basket_dict
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        }

@router.post("/baskets/", status_code=status.HTTP_201_CREATED)
async def create_basket(
    token: str,
    name: str,
    style_data: Dict[str, StyleData] = Body(None),
    db: Session = Depends(get_db)
):
    try:
        user_id = decode_token(token)
        
        # Check if user exists
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            return {
                "success": False,
                "message": "User not found",
                "data": None
            }
        
        # Extract style_ids and style_infos from style_data
        style_ids = []
        style_infos = {}
        
        if style_data:
            style_ids = list(style_data.keys())
            style_infos = {k: v.dict() for k, v in style_data.items()}
        
        # Create basket
        db_basket = Basket(
            user_id=user_id,
            name=name,
            style_ids=style_ids,
            style_infos=style_infos
        )
        
        db.add(db_basket)
        db.commit()
        db.refresh(db_basket)
        
        basket_dict = db_basket.__dict__
        basket_dict.pop("user_id")
        if "_sa_instance_state" in basket_dict:
            basket_dict.pop("_sa_instance_state")
        
        return {
            "success": True,
            "message": None,
            "data": basket_dict
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        }

@router.put("/baskets/{name}")
async def update_basket(
    name: str,
    token: str,
    style_data: Dict[str, StyleData] = Body(...),  # Required body
    db: Session = Depends(get_db)
):
    try:
        user_id = decode_token(token)
        
        # Check if user exists
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            return {
                "success": False,
                "message": "User not found",
                "data": None
            }
        
        # Get the basket by user_id and name
        basket = db.query(Basket).filter(Basket.user_id == user_id, Basket.name == name).first()
        if basket is None:
            return {
                "success": False,
                "message": "Basket not found",
                "data": None
            }
        
        # Get existing style data
        existing_style_ids = basket.style_ids
        existing_style_infos = basket.style_infos
        
        # Extract new style data
        new_style_ids = list(style_data.keys())
        new_style_infos = {k: v.dict() for k, v in style_data.items()}
        
        # Merge style data
        merged_style_ids = list(set(existing_style_ids + new_style_ids))
        merged_style_infos = {**existing_style_infos, **new_style_infos}
        
        # Update basket fields
        basket.style_ids = merged_style_ids
        basket.style_infos = merged_style_infos
        
        db.commit()
        db.refresh(basket)
        
        basket_dict = basket.__dict__
        basket_dict.pop("user_id")
        if "_sa_instance_state" in basket_dict:
            basket_dict.pop("_sa_instance_state")
        
        return {
            "success": True,
            "message": None,
            "data": basket_dict
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        }

@router.delete("/baskets/{name}")
async def delete_basket(name: str, token: str, db: Session = Depends(get_db)):
    try:
        user_id = decode_token(token)
        
        # Check if user exists
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            return {
                "success": False,
                "message": "User not found",
                "data": None
            }
        
        # Get the basket by user_id and name
        basket = db.query(Basket).filter(Basket.user_id == user_id, Basket.name == name).first()
        if basket is None:
            return {
                "success": False,
                "message": "Basket not found",
                "data": None
            }
        
        db.delete(basket)
        db.commit()
        
        return {
            "success": True,
            "message": None,
            "data": {"name": name}
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        } 