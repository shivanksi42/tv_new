from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import requests
from collections import defaultdict
import asyncio
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

BACKEND_BASE_URL = os.getenv("BACKEND_BASE_URL", "https://api.staging.tracevenue.com")
FILTERED_VARIANTS_ENDPOINT = os.getenv("FILTERED_VARIANTS_ENDPOINT", "api/v1/traceVenue/variant/filteredVariants")

app = FastAPI(title="Restaurant Menu Analysis API", version="1.0.0")

class AnalysisResult(BaseModel):
    item_id: str
    item_name: str
    missing_percentage: float
    restaurants_not_offering: int
    total_restaurants: int

class ProcessedData(BaseModel):
    total_variants: int
    total_restaurants: int
    missing_items: List[AnalysisResult]

class MenuAnalyzer:
    def __init__(self):
        self.restaurant_items = defaultdict(set)  
        self.item_names = {}  
        self.variant_data = []
    
    def extract_variant_data(self, api_response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract variant ID, menu item IDs, and venue ID from API response"""
        extracted_variants = []
        
        variants = api_response.get("variants", [])
        
        for variant in variants:
            variant_id = variant.get("_id")
            venue_id = variant.get("venueId")
            menu_items = variant.get("menuItems", [])
            
            item_ids = []
            for item in menu_items:
                item_id = item.get("_id")
                item_name = item.get("name", "Unknown")
                if item_id:
                    item_ids.append(item_id)
                    self.item_names[item_id] = item_name
            
            extracted_data = {
                "variant_id": variant_id,
                "venue_id": venue_id,
                "menu_item_ids": item_ids
            }
            
            extracted_variants.append(extracted_data)
            self.variant_data.append(extracted_data)
        
        return extracted_variants
    
    def group_items_by_restaurant(self):
        """Group all available items by restaurant (venue_id)"""
        for variant in self.variant_data:
            venue_id = variant["venue_id"]
            item_ids = variant["menu_item_ids"]
            
            self.restaurant_items[venue_id].update(item_ids)
    
    def calculate_missing_items_analysis(self) -> List[AnalysisResult]:
        """Calculate which items are missing from restaurants and their percentages"""
        if not self.restaurant_items:
            return []
        
        all_items = set()
        for items in self.restaurant_items.values():
            all_items.update(items)
        
        total_restaurants = len(self.restaurant_items)
        missing_items_analysis = []
        
        for item_id in all_items:
            restaurants_offering = 0
            
            for venue_items in self.restaurant_items.values():
                if item_id in venue_items:
                    restaurants_offering += 1
            
            restaurants_not_offering = total_restaurants - restaurants_offering
            
            if restaurants_not_offering > 0:
                missing_percentage = (restaurants_not_offering / total_restaurants) * 100
                
                analysis_result = AnalysisResult(
                    item_id=item_id,
                    item_name=self.item_names.get(item_id, "Unknown"),
                    missing_percentage=round(missing_percentage, 2),
                    restaurants_not_offering=restaurants_not_offering,
                    total_restaurants=total_restaurants
                )
                
                missing_items_analysis.append(analysis_result)
        
        missing_items_analysis.sort(key=lambda x: x.missing_percentage, reverse=True)
        
        return missing_items_analysis

analyzer = MenuAnalyzer()

@app.post("/analyze-menu", response_model=ProcessedData)
async def analyze_menu_data(api_response: Dict[str, Any]):
    """
    Analyze menu data from external API response.
    
    This endpoint:
    1. Extracts variant IDs and menu item IDs
    2. Groups items by restaurant (venue_id)
    3. Calculates missing items statistics
    4. Returns items not offered by all restaurants with percentages
    """
    try:
        global analyzer
        analyzer = MenuAnalyzer()
        
        extracted_variants = analyzer.extract_variant_data(api_response)
        
        if not extracted_variants:
            raise HTTPException(status_code=400, detail="No variants found in the API response")
        
        analyzer.group_items_by_restaurant()
        
        missing_items = analyzer.calculate_missing_items_analysis()
        
        return ProcessedData(
            total_variants=len(extracted_variants),
            total_restaurants=len(analyzer.restaurant_items),
            missing_items=missing_items
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")

@app.get("/fetch-and-analyze")
async def fetch_and_analyze(query_params: Optional[str] = None):
    """
    Fetch data from TraceVenue API and analyze it.
    
    Args:
        query_params: Optional query parameters to append to the API call (e.g., "?city=Mumbai&category=restaurant")
    """
    try:
        api_url = f"{BACKEND_BASE_URL}/{FILTERED_VARIANTS_ENDPOINT}"
        if query_params:
            api_url += f"?{query_params}"
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(api_url)
            response.raise_for_status()
            api_data = response.json()
        
        return await analyze_menu_data(api_data)
        
    except httpx.RequestError as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing fetched data: {str(e)}")

@app.post("/fetch-and-analyze-with-filters")
async def fetch_and_analyze_with_filters(filters: Dict[str, Any] = None):
    """
    Fetch data from TraceVenue API with POST filters and analyze it.
    
    Args:
        filters: Dictionary of filters to send as JSON body to the API
    """
    try:
        api_url = f"{BACKEND_BASE_URL}/{FILTERED_VARIANTS_ENDPOINT}"
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            if filters:
                response = await client.post(api_url, json=filters)
            else:
                response = await client.get(api_url)
            
            response.raise_for_status()
            api_data = response.json()
        
        return await analyze_menu_data(api_data)
        
    except httpx.RequestError as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing fetched data: {str(e)}")

@app.get("/restaurant-summary")
async def get_restaurant_summary():
    """Get summary of processed restaurant data"""
    if not analyzer.restaurant_items:
        raise HTTPException(status_code=404, detail="No data processed yet. Use /analyze-menu endpoint first.")
    summary = {}
    for venue_id, items in analyzer.restaurant_items.items():
        summary[venue_id] = {
            "total_items": len(items),
            "item_ids": list(items)
        }
    
    return {
        "total_restaurants": len(analyzer.restaurant_items),
        "restaurants": summary
    }

@app.get("/")
async def root():
    return {
        "message": "Restaurant Menu Analysis API",
        "tracevenue_api": f"{BACKEND_BASE_URL}/{FILTERED_VARIANTS_ENDPOINT}",
        "endpoints": {
            "POST /analyze-menu": "Analyze menu data from API response JSON",
            "GET /fetch-and-analyze": "Fetch and analyze data from TraceVenue API (with optional query params)",
            "POST /fetch-and-analyze-with-filters": "Fetch and analyze data from TraceVenue API with POST filters",
            "GET /restaurant-summary": "Get summary of processed restaurant data"
        },
        "usage_examples": {
            "fetch_with_query": "GET /fetch-and-analyze?query_params=city=Mumbai&category=restaurant",
            "fetch_with_filters": "POST /fetch-and-analyze-with-filters with JSON body containing filters"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)