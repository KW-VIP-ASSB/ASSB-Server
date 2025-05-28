from typing import Any, Dict, List, Optional
import httpx
import os
from mcp.server.fastmcp import FastMCP

# Initialize FastMCP server
mcp = FastMCP("superbasket-mcp")

# Constants
API_BASE_URL = "http://localhost:8000/api"
session = os.getenv("SESSION")

@mcp.tool()
async def get_baskets(skip: int = 0, limit: int = 100) -> Dict[str, Any]:
    """Get all baskets for the current user.
    
    Args:
        skip: Number of records to skip (for pagination)
        limit: Maximum number of records to return
    """
    if not session:
        return {"success": False, "message": "No session token found", "data": None}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{API_BASE_URL}/baskets/",
                params={"token": session, "skip": skip, "limit": limit}
            )
            return response.json()
        except Exception as e:
            return {"success": False, "message": str(e), "data": None}

@mcp.tool()
async def get_basket(name: str) -> Dict[str, Any]:
    """Get a specific basket by name.
    
    Args:
        name: Name of the basket to retrieve
    """
    if not session:
        return {"success": False, "message": "No session token found", "data": None}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                f"{API_BASE_URL}/baskets/{name}",
                params={"token": session}
            )
            return response.json()
        except Exception as e:
            return {"success": False, "message": str(e), "data": None}

@mcp.tool()
async def create_basket(name: str, style_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create a new basket.
    
    Args:
        name: Name of the basket to create
        style_data: Optional style data to include in the basket
    """
    if not session:
        return {"success": False, "message": "No session token found", "data": None}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{API_BASE_URL}/baskets/",
                params={"token": session},
                json={"name": name, "style_data": style_data}
            )
            return response.json()
        except Exception as e:
            return {"success": False, "message": str(e), "data": None}

@mcp.tool()
async def update_basket(name: str, style_data: Dict[str, Any]) -> Dict[str, Any]:
    """Update an existing basket.
    
    Args:
        name: Name of the basket to update
        style_data: Style data to update in the basket
    """
    if not session:
        return {"success": False, "message": "No session token found", "data": None}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.put(
                f"{API_BASE_URL}/baskets/{name}",
                params={"token": session},
                json={"style_data": style_data}
            )
            return response.json()
        except Exception as e:
            return {"success": False, "message": str(e), "data": None}

@mcp.tool()
async def delete_basket(name: str) -> Dict[str, Any]:
    """Delete a basket.
    
    Args:
        name: Name of the basket to delete
    """
    if not session:
        return {"success": False, "message": "No session token found", "data": None}
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.delete(
                f"{API_BASE_URL}/baskets/{name}",
                params={"token": session}
            )
            return response.json()
        except Exception as e:
            return {"success": False, "message": str(e), "data": None}

# Add prompts
@mcp.prompt()
def basket_management_prompt() -> str:
    """A prompt for managing baskets."""
    return """
    You can manage your baskets using the following commands:
    
    1. List all baskets: Use get_baskets()
    2. Get a specific basket: Use get_basket(name="basket_name")
    3. Create a new basket: Use create_basket(name="basket_name", style_data={...})
    4. Update a basket: Use update_basket(name="basket_name", style_data={...})
    5. Delete a basket: Use delete_basket(name="basket_name")
    
    Each basket has a unique name per user and can contain style data.
    """

if __name__ == "__main__":
    mcp.run(transport='stdio')
