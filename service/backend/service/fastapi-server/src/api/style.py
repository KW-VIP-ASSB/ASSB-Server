from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import select, desc
from typing import Optional, Dict, Any, List
from collections import defaultdict
from pydantic import BaseModel

from src.db.database import get_db
from src.db.schema.style import Style, StylePrice, StyleImage, StyleMetadata
from src.db.schema.facets import StyleFacet, Facet
from src.db.schema.reviews import Review, StyleReview

router = APIRouter(tags=["styles"])

class StyleRequest(BaseModel):
    site_id: str
    style_id: str

@router.post("/styles/", response_model=Dict[str, Any])
async def get_style(
    style_requests: List[StyleRequest],
    db: Session = Depends(get_db)
):
    """
    Get styles by list of site_id and style_id pairs
    """
    response = {}
    
    for request in style_requests:
        site_id = request.site_id
        style_id = request.style_id
        
        try:
            # Validate input parameters
            if not site_id or not style_id:
                response[str(style_id)] = {
                    "success": False, 
                    "message": "Invalid site_id or style_id"
                }
                continue
            
            # Query style table with better error handling
            style_query = select(
                Style.id, 
                Style.style_id, 
                Style.name, 
                Style.url
            ).where(
                Style.site_id == site_id,
                Style.style_id == style_id
            )
            
            style_result = db.execute(style_query).first()
            
            if not style_result:
                # Style not found, include only success: false
                response[str(style_id)] = {"success": False}
                continue
            
            # Safe indexing with validation
            style_id_value = style_result[0] if len(style_result) > 0 else None
            style_idx = style_result[1] if len(style_result) > 1 else None
            style_name = style_result[2] if len(style_result) > 2 else None
            style_url = style_result[3] if len(style_result) > 3 else None
            
            if style_id_value is None:
                response[str(style_id)] = {
                    "success": False, 
                    "message": "Invalid style data"
                }
                continue
            
            # Query most recent price with error handling
            price_data = {"original_price": None, "price": None, "currency": None}
            try:
                price_query = select(
                    StylePrice.original_price,
                    StylePrice.price,
                    StylePrice.currency
                ).where(
                    StylePrice.site_id == site_id,
                    StylePrice.style_id == style_id_value
                ).order_by(desc(StylePrice.date)).limit(1)
                
                price_result = db.execute(price_query).first()
                
                if price_result and len(price_result) >= 3:
                    price_data = {
                        "original_price": price_result[0],
                        "price": price_result[1],
                        "currency": price_result[2]
                    }
            except Exception as e:
                # Log the error but continue processing
                print(f"Error fetching price for style {style_id}: {e}")
            
            # Query most recent image with error handling
            image_data = {"origin": None}
            try:
                image_query = select(
                    StyleImage.origin
                ).where(
                    StyleImage.site_id == site_id,
                    StyleImage.style_id == style_id_value
                ).order_by(desc(StyleImage.created_at)).limit(1)
                
                image_result = db.execute(image_query).first()
                
                if image_result and len(image_result) > 0:
                    image_data = {"origin": image_result[0]}
            except Exception as e:
                print(f"Error fetching image for style {style_id}: {e}")
            
            # Query style metadata with error handling
            metadata_data = {"description": None, "data": {}}
            try:
                metadata_query = select(
                    StyleMetadata.description,
                    StyleMetadata.data
                ).where(
                    StyleMetadata.site_id == site_id,
                    StyleMetadata.id == style_id_value
                )
                
                metadata_result = db.execute(metadata_query).first()
                
                if metadata_result and len(metadata_result) >= 2:
                    metadata_data = {
                        "description": metadata_result[0],
                        "data": metadata_result[1] if metadata_result[1] is not None else {}
                    }
            except Exception as e:
                print(f"Error fetching metadata for style {style_id}: {e}")
            
            # Query facets with comprehensive error handling
            facet_groups = {}
            try:
                # Step 1: Get all facet_ids for this style
                facet_ids_query = select(StyleFacet.facet_id).where(
                    StyleFacet.site_id == site_id,
                    StyleFacet.style_id == style_id_value
                )
                facet_ids_result = db.execute(facet_ids_query).fetchall()
                
                # Extract facet_ids from result with safe indexing
                facet_ids = []
                for row in facet_ids_result:
                    if row and len(row) > 0 and row[0] is not None:
                        facet_ids.append(row[0])
                
                # Step 2: Get facet details for these ids
                if facet_ids:
                    facets_query = select(
                        Facet.id,
                        Facet.type,
                        Facet.name
                    ).where(
                        Facet.id.in_(facet_ids)
                    )
                    facets_result = db.execute(facets_query).fetchall()
                    
                    # Group facets by type with safe indexing
                    facet_groups_temp = defaultdict(list)
                    for facet in facets_result:
                        if facet and len(facet) >= 3:
                            facet_type = facet[1]
                            facet_name = facet[2]
                            
                            if facet_type is not None and facet_name is not None:
                                facet_groups_temp[facet_type].append(facet_name)
                    
                    facet_groups = dict(facet_groups_temp)
            except Exception as e:
                print(f"Error fetching facets for style {style_id}: {e}")
            
            # Query reviews with error handling
            reviews = []
            try:
                reviews_query = select(
                    Review.review_id,
                    Review.text,
                ).join(
                    StyleReview,
                    StyleReview.review_id == Review.id
                ).where(
                    StyleReview.site_id == site_id,
                    StyleReview.style_id == style_id_value
                ).order_by(
                    desc(Review.writed_at)
                ).limit(10)
                
                reviews_result = db.execute(reviews_query).fetchall()
                
                # Format reviews with safe indexing
                for review in reviews_result:
                    if review and len(review) >= 2:
                        review_data = {
                            "review_id": review[0],
                            "text": review[1] if review[1] is not None else "",
                        }
                        reviews.append(review_data)
            except Exception as e:
                print(f"Error fetching reviews for style {style_id}: {e}")

            # Format the response for this style - convert style_id_value to string
            response[str(style_id_value)] = {
                "success": True,
                "style_idx": style_idx,
                "site_id": site_id,
                "name": style_name,
                "url": style_url,
                "price": price_data,
                "image": image_data,
                "metadata": metadata_data,
                "facets": facet_groups,
                "reviews": reviews
            }
            
        except Exception as e:
            # Handle any exception that might occur during processing
            print(f"Unexpected error processing style {style_id}: {e}")
            response[str(style_id)] = {
                "success": False, 
                "message": f"Internal server error: {str(e)}"
            }
    
    return response
