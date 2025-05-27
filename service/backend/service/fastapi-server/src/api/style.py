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
            # Query style table
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
            
            style_id_value = style_result[0]  # The id from Style table
            
            # Query most recent price
            price_query = select(
                StylePrice.original_price,
                StylePrice.price,
                StylePrice.currency
            ).where(
                StylePrice.site_id == site_id,
                StylePrice.style_id == style_id_value
            ).order_by(desc(StylePrice.date)).limit(1)
            
            price_result = db.execute(price_query).first()
            
            # Query most recent image
            image_query = select(
                StyleImage.origin
            ).where(
                StyleImage.site_id == site_id,
                StyleImage.style_id == style_id_value
            ).order_by(desc(StyleImage.created_at)).limit(1)
            
            image_result = db.execute(image_query).first()
            
            # Query style metadata
            metadata_query = select(
                StyleMetadata.description,
                StyleMetadata.data
            ).where(
                StyleMetadata.site_id == site_id,
                StyleMetadata.id == style_id_value
            )
            
            metadata_result = db.execute(metadata_query).first()
            
            # Step 1: Get all facet_ids for this style
            facet_ids_query = select(StyleFacet.facet_id).where(
                StyleFacet.site_id == site_id,
                StyleFacet.style_id == style_id_value
            )
            facet_ids_result = db.execute(facet_ids_query).fetchall()
            
            # Extract facet_ids from result
            facet_ids = [row[0] for row in facet_ids_result]
            
            # Skip the assertion to handle styles without facets
            # and set default empty facet_groups
            facet_groups = {}
            
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
                
                # Group facets by type
                facet_groups = defaultdict(list)
                for facet in facets_result:
                    facet_type = facet[1]
                    facet_name = facet[2]
                    
                    facet_groups[facet_type].append(facet_name)
            
            # Query reviews
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
            
            # Format reviews
            reviews = []
            for review in reviews_result:
                reviews.append({
                    "review_id": review[0],
                    "author_name": review[1],
                    "rating": review[2],
                    "recommended": review[3],
                    "verified_purchaser": review[4],
                    "title": review[5],
                    "text": review[6],
                    "writed_at": review[7].isoformat() if review[7] else None
                })

            # Format the response for this style - convert style_id_value to string
            response[str(style_id_value)] = {
                "success": True,
                "style_idx": style_result[1],  # style_id from the Style table
                "site_id": site_id,
                "name": style_result[2],
                "url": style_result[3],
                "price": {
                    "original_price": price_result[0] if price_result else None,
                    "price": price_result[1] if price_result else None,
                    "currency": price_result[2] if price_result else None
                },
                "image": {
                    "origin": image_result[0] if image_result else None
                },
                "metadata": {
                    "description": metadata_result[0] if metadata_result else None,
                    "data": metadata_result[1] if metadata_result else {}
                },
                "facets": dict(facet_groups),
                "reviews": reviews
            }
        except Exception as e:
            # Handle any exception that might occur during processing
            response[str(style_id)] = {"success": False, "message": str(e)}
    
    return response
