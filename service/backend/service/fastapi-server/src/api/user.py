from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from src.db.database import get_db
from src.db.schema.user import User, UserCreate, UserUpdate, UserResponse
from passlib.context import CryptContext
from datetime import datetime
import base64
from pydantic import BaseModel, EmailStr

router = APIRouter(tags=["users"])
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def decode_token(token: str) -> int:
    """Decode the base64 token to get the user ID"""
    try:
        decoded_bytes = base64.b64decode(token)
        return int(decoded_bytes.decode('utf-8'))
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

class UserLogin(BaseModel):
    email: EmailStr
    password: str

@router.get("/users/")
async def read_users(
    skip: int = 0, 
    limit: int = 100, 
    db: Session = Depends(get_db)
):
    try:
        users = db.query(User).offset(skip).limit(limit).all()
        user_list = [user.__dict__ for user in users]
        for user_dict in user_list:
            if "_sa_instance_state" in user_dict:
                user_dict.pop("_sa_instance_state")
            if "hashed_password" in user_dict:
                user_dict.pop("hashed_password")
        
        return {
            "success": True,
            "message": None,
            "data": user_list
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        }

@router.get("/users/{token}")
async def read_user(token: str, db: Session = Depends(get_db)):
    try:
        user_id = decode_token(token)
        
        user = db.query(User).filter(User.id == user_id).first()
        if user is None:
            return {
                "success": False,
                "message": "User not found",
                "data": None
            }
        
        user_dict = user.__dict__
        user_dict.pop("id")
        if "_sa_instance_state" in user_dict:
            user_dict.pop("_sa_instance_state")
        if "hashed_password" in user_dict:
            user_dict.pop("hashed_password")
        
        return {
            "success": True,
            "message": None,
            "data": user_dict
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        }

@router.post("/users/", status_code=status.HTTP_201_CREATED)
async def create_user(user: UserCreate, db: Session = Depends(get_db)):
    try:
        # Check if user with same email exists
        db_user_by_email = db.query(User).filter(User.email == user.email).first()
        if db_user_by_email:
            return {
                "success": False,
                "message": "Email already registered",
                "data": None
            }
        
        # Hash the password
        hashed_password = pwd_context.hash(user.password)
        
        # Create user object with current timestamp for updated_at
        now = datetime.now()
        db_user = User(
            username=user.username,
            email=user.email,
            hashed_password=hashed_password,
            is_active=user.is_active,
            updated_at=now
        )
        
        # Save to database
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        
        user_dict = db_user.__dict__
        if "_sa_instance_state" in user_dict:
            user_dict.pop("_sa_instance_state")
        if "hashed_password" in user_dict:
            user_dict.pop("hashed_password")
        
        return {
            "success": True,
            "message": None,
            "data": user_dict
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        }

@router.put("/users/{token}")
async def update_user(token: str, user: UserUpdate, db: Session = Depends(get_db)):
    try:
        user_id = decode_token(token)
        
        db_user = db.query(User).filter(User.id == user_id).first()
        if db_user is None:
            return {
                "success": False,
                "message": "User not found",
                "data": None
            }
        
        # Update user data if provided
        update_data = user.dict(exclude_unset=True)
        
        # If password is provided, hash it
        if "password" in update_data:
            update_data["hashed_password"] = pwd_context.hash(update_data.pop("password"))
        
        # Check email uniqueness if updating email
        if "email" in update_data and update_data["email"] != db_user.email:
            email_exists = db.query(User).filter(User.email == update_data["email"]).first()
            if email_exists:
                return {
                    "success": False,
                    "message": "Email already exists",
                    "data": None
                }
        
        # Update user attributes
        for key, value in update_data.items():
            setattr(db_user, key, value)
        
        db.commit()
        db.refresh(db_user)
        
        user_dict = db_user.__dict__
        if "_sa_instance_state" in user_dict:
            user_dict.pop("_sa_instance_state")
        if "hashed_password" in user_dict:
            user_dict.pop("hashed_password")
        
        return {
            "success": True,
            "message": None,
            "data": user_dict
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        }

@router.delete("/users/{token}", status_code=status.HTTP_200_OK)
async def delete_user(token: str, db: Session = Depends(get_db)):
    try:
        user_id = decode_token(token)
        
        db_user = db.query(User).filter(User.id == user_id).first()
        if db_user is None:
            return {
                "success": False,
                "message": "User not found",
                "data": None
            }
        
        db.delete(db_user)
        db.commit()
        
        return {
            "success": True,
            "message": None,
            "data": {"id": user_id}
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        }

@router.post("/users/login")
async def login_user(user_login: UserLogin, db: Session = Depends(get_db)):
    try:
        # Find user by email
        db_user = db.query(User).filter(User.email == user_login.email).first()
        if db_user is None:
            return {
                "success": False,
                "message": "Invalid credentials",
                "data": None
            }
        
        # Verify password
        if not pwd_context.verify(user_login.password, db_user.hashed_password):
            return {
                "success": False,
                "message": "Invalid credentials",
                "data": None
            }
        
        # Encode user ID
        user_id_str = str(db_user.id)
        encoded_id = base64.b64encode(user_id_str.encode()).decode()
        
        return {
            "success": True,
            "message": None,
            "data": {"token": encoded_id}
        }
    except Exception as e:
        return {
            "success": False,
            "message": str(e),
            "data": None
        } 