from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Set
from collections import defaultdict, Counter
import asyncio
import httpx
import os
from dotenv import load_dotenv
import time
from concurrent.futures import ThreadPoolExecutor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

BACKEND_BASE_URL = os.getenv("BACKEND_BASE_URL", "https://api.staging.tracevenue.com")
FILTERED_VARIANTS_ENDPOINT = os.getenv("FILTERED_VARIANTS_ENDPOINT", "api/v1/traceVenue/variant/filteredVariants")

app = FastAPI(title="Restaurant Menu Analysis API", version="2.0.0")

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
    processing_time: float
    

class OptimizedMenuAnalyzer:
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset analyzer state for new analysis"""
        self.restaurant_items: Dict[str, Set[str]] = defaultdict(set)
        self.item_names: Dict[str, str] = {}
        self.all_items: Set[str] = set()
    
    def extract_and_process_variants(self, api_response: Dict[str, Any]) -> tuple:
        """Optimized single-pass extraction and processing with debugging"""
        # Log the structure of the API response for debugging
        logger.info(f"API Response keys: {list(api_response.keys())}")
        
        variants = api_response.get("variants", [])
        
        # Check for alternative response structures
        if not variants:
            # Try common alternative keys
            variants = api_response.get("data", {}).get("variants", [])
            if not variants:
                variants = api_response.get("result", {}).get("variants", [])
            if not variants:
                variants = api_response.get("items", [])
        
        logger.info(f"Found {len(variants)} variants to process")
        
        if not variants:
            logger.warning("No variants found in API response")
            return 0, 0
        
        # Single pass through variants
        processed_variants = 0
        for variant in variants:
            venue_id = variant.get("venueId") or variant.get("venue_id") or variant.get("restaurantId")
            if not venue_id:
                logger.debug(f"Skipping variant without venue ID: {list(variant.keys())}")
                continue
                
            menu_items = variant.get("menuItems", []) or variant.get("menu_items", []) or variant.get("items", [])
            
            # Process menu items in batch
            venue_items = set()
            for item in menu_items:
                item_id = item.get("_id") or item.get("id") or item.get("itemId")
                if item_id:
                    venue_items.add(item_id)
                    self.item_names[item_id] = item.get("name", "Unknown")
                    self.all_items.add(item_id)
            
            # Union operation is faster than multiple updates
            if venue_items:  # Only add if there are items
                self.restaurant_items[venue_id].update(venue_items)
                processed_variants += 1
        
        logger.info(f"Processed {processed_variants} variants with menu items")
        logger.info(f"Total unique restaurants: {len(self.restaurant_items)}")
        logger.info(f"Total unique items: {len(self.all_items)}")
        
        return len(variants), len(self.restaurant_items)
    
    def calculate_missing_items_optimized(self) -> List[AnalysisResult]:
        """Optimized missing items calculation using set operations"""
        if not self.restaurant_items:
            logger.warning("No restaurant data available for analysis")
            return []
        
        total_restaurants = len(self.restaurant_items)
        restaurant_sets = list(self.restaurant_items.values())
        
        # Count items per restaurant using Counter for efficiency
        item_restaurant_count = Counter()
        for restaurant_items in restaurant_sets:
            for item_id in restaurant_items:
                item_restaurant_count[item_id] += 1
        
        logger.info(f"Item distribution calculated for {len(item_restaurant_count)} unique items")
        
        missing_items_analysis = []
        
        # Process items that are missing from at least one restaurant
        for item_id, count in item_restaurant_count.items():
            if count < total_restaurants:
                restaurants_not_offering = total_restaurants - count
                missing_percentage = (restaurants_not_offering / total_restaurants) * 100
                
                missing_items_analysis.append(AnalysisResult(
                    item_id=item_id,
                    item_name=self.item_names.get(item_id, "Unknown"),
                    missing_percentage=round(missing_percentage, 2),
                    restaurants_not_offering=restaurants_not_offering,
                    total_restaurants=total_restaurants
                ))
        
        # Sort by missing percentage (descending)
        missing_items_analysis.sort(key=lambda x: x.missing_percentage, reverse=True)
        
        logger.info(f"Found {len(missing_items_analysis)} items with availability gaps")
        
        return missing_items_analysis

# Thread pool for CPU-intensive tasks
executor = ThreadPoolExecutor(max_workers=4)

async def process_data_async(api_response: Dict[str, Any]) -> ProcessedData:
    """Process data asynchronously in thread pool"""
    def process_data():
        analyzer = OptimizedMenuAnalyzer()
        start_time = time.time()
        
        total_variants, total_restaurants = analyzer.extract_and_process_variants(api_response)
        missing_items = analyzer.calculate_missing_items_optimized()
        
        processing_time = round(time.time() - start_time, 3)
        
       
        
        return ProcessedData(
            total_variants=total_variants,
            total_restaurants=total_restaurants,
            missing_items=missing_items,
            processing_time=processing_time,
        )
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, process_data)

@app.post("/analyze-menu", response_model=ProcessedData)
async def analyze_menu_data(api_response: Dict[str, Any]):
    """
    Analyze menu data from external API response with optimized processing.
    """
    try:
        logger.info("Starting menu analysis")
        return await process_data_async(api_response)
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")

@app.get("/fetch-and-analyze")
async def fetch_and_analyze(query_params: Optional[str] = None):
    """
    Fetch data from TraceVenue API and analyze it (no caching).
    """
    try:
        api_url = f"{BACKEND_BASE_URL}/{FILTERED_VARIANTS_ENDPOINT}"
        if query_params:
            api_url += f"?{query_params}"
        
        logger.info(f"Fetching fresh data from: {api_url}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(api_url)
            response.raise_for_status()
            api_data = response.json()
        
        logger.info(f"Received API response with keys: {list(api_data.keys())}")
        
        result = await process_data_async(api_data)
        return result
        
    except httpx.RequestError as e:
        logger.error(f"API request error: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        logger.error(f"Processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing fetched data: {str(e)}")

@app.post("/fetch-and-analyze-with-filters")
async def fetch_and_analyze_with_filters(filters: Dict[str, Any] = None):
    """
    Fetch data from TraceVenue API with POST filters and analyze it (no caching).
    """
    try:
        api_url = f"{BACKEND_BASE_URL}/{FILTERED_VARIANTS_ENDPOINT}"
        
        logger.info(f"Fetching fresh data from: {api_url}")
        if filters:
            logger.info(f"With filters: {filters}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            if filters:
                response = await client.post(api_url, json=filters)
            else:
                response = await client.get(api_url)
            
            response.raise_for_status()
            api_data = response.json()
        
        logger.info(f"Received API response with keys: {list(api_data.keys())}")
        
        result = await process_data_async(api_data)
        return result
        
    except httpx.RequestError as e:
        logger.error(f"API request error: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        logger.error(f"Processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing fetched data: {str(e)}")



@app.get("/debug/test-data-structure")
async def test_data_structure(query_params: Optional[str] = None):
    """Debug endpoint to inspect API response structure without processing"""
    try:
        api_url = f"{BACKEND_BASE_URL}/{FILTERED_VARIANTS_ENDPOINT}"
        if query_params:
            api_url += f"?{query_params}"
        
        logger.info(f"Testing API structure from: {api_url}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(api_url)
            response.raise_for_status()
            api_data = response.json()
        
        # Return structure information
        def analyze_structure(obj, max_depth=3, current_depth=0):
            if current_depth >= max_depth:
                return "..."
            
            if isinstance(obj, dict):
                return {k: analyze_structure(v, max_depth, current_depth + 1) for k, v in list(obj.items())[:5]}
            elif isinstance(obj, list):
                if len(obj) > 0:
                    return [analyze_structure(obj[0], max_depth, current_depth + 1), f"... {len(obj)} items total"]
                return []
            else:
                return type(obj).__name__
        
        return {
            "api_url": api_url,
            "response_size_kb": round(len(str(api_data)) / 1024, 2),
            "response_structure": analyze_structure(api_data),
            "top_level_keys": list(api_data.keys()) if isinstance(api_data, dict) else "Not a dict",
            "variants_count": len(api_data.get("variants", [])),
            "sample_variant": api_data.get("variants", [{}])[0] if api_data.get("variants") else None,
            "raw_response_preview": str(api_data)[:500] + "..." if len(str(api_data)) > 500 else str(api_data)
        }
        
    except Exception as e:
        logger.error(f"Debug error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Debug error: {str(e)}")

@app.get("/health")
async def health_check():
    """Simple health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "api_configured": bool(BACKEND_BASE_URL and FILTERED_VARIANTS_ENDPOINT)
    }

@app.get("/")
async def root():
    return {
        "message": "Clean Restaurant Menu Analysis API (No Caching)",
        "version": "2.0.0",
        "tracevenue_api": f"{BACKEND_BASE_URL}/{FILTERED_VARIANTS_ENDPOINT}",
        "features": [
            "Real-time data processing",
            "Async processing with thread pools",
            "Optimized set operations",
            "Single-pass data processing",
            "Counter-based calculations",
            "Enhanced debugging and logging",
            "No caching - always fresh data"
        ],
        "endpoints": {
            "POST /analyze-menu": "Analyze menu data from API response JSON",
            "GET /fetch-and-analyze": "Fetch and analyze data from TraceVenue API",
            "POST /fetch-and-analyze-with-filters": "Fetch and analyze data with POST filters",
            "GET /debug/test-data-structure": "Debug API response structure",
            "GET /health": "Health check endpoint"
        },
        "benefits_of_no_caching": [
            "Always fresh data",
            "No stale results",
            "Easier debugging",
            "Lower memory usage",
            "Simpler architecture"
        ],
        "usage_examples": {
            "analyze_direct": "POST /analyze-menu with your API response JSON",
            "fetch_with_filters": "POST /fetch-and-analyze-with-filters with your filters",
            "debug_structure": "GET /debug/test-data-structure to inspect API response"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        workers=1,
        log_level="info"
    )