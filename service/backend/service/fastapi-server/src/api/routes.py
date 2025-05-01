from fastapi import APIRouter
from src.api.user import router as user_router
from src.api.basket import router as basket_router
from src.api.style import router as style_router

router = APIRouter()

# Include routers from separate files
router.include_router(user_router)
router.include_router(basket_router)
router.include_router(style_router) 