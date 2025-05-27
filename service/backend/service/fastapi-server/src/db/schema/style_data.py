from pydantic import BaseModel
from typing import Dict, Any, Optional

class StyleData(BaseModel):
    """
    Shared model for style data across the application
    """
    style_idx: str
    site_id: str
    name: str
    url: str
    data: Optional[Dict[str, Any]] = {}
    price: Dict[str, Any]
    image: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = {}
    facets: Dict[str, Any]
    success: Optional[bool] = True 