from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Set
import requests
from collections import defaultdict, Counter
import asyncio
import httpx
import os
from dotenv import load_dotenv
import time
from concurrent.futures import ThreadPoolExecutor
import threading
from functools import lru_cache

load_dotenv()

BACKEND_BASE_URL = os.getenv("BACKEND_BASE_URL", "https://api.staging.tracevenue.com")
FILTERED_VARIANTS_ENDPOINT = os.getenv("FILTERED_VARIANTS_ENDPOINT", "api/v1/traceVenue/variant/filteredVariants")

app = FastAPI(title="Restaurant Menu Analysis API", version="1.0.0")

_cache = {}
_cache_lock = threading.Lock()
CACHE_TTL = 300  

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
    cache_hit: bool = False

class OptimizedMenuAnalyzer:
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset analyzer state for new analysis"""
        self.restaurant_items: Dict[str, Set[str]] = defaultdict(set)
        self.item_names: Dict[str, str] = {}
        self.all_items: Set[str] = set()
    
    def extract_and_process_variants(self, api_response: Dict[str, Any]) -> tuple:
        """Optimized single-pass extraction and processing"""
        variants = api_response.get("variants", [])
        
        if not variants:
            return 0, 0
        
        # Single pass through variants
        for variant in variants:
            venue_id = variant.get("venueId")
            if not venue_id:
                continue
                
            menu_items = variant.get("menuItems", [])
            
            # Process menu items in batch
            venue_items = set()
            for item in menu_items:
                item_id = item.get("_id")
                if item_id:
                    venue_items.add(item_id)
                    self.item_names[item_id] = item.get("name", "Unknown")
                    self.all_items.add(item_id)
            
            # Union operation is faster than multiple updates
            self.restaurant_items[venue_id].update(venue_items)
        
        return len(variants), len(self.restaurant_items)
    
    def calculate_missing_items_optimized(self) -> List[AnalysisResult]:
        """Optimized missing items calculation using set operations"""
        if not self.restaurant_items:
            return []
        
        total_restaurants = len(self.restaurant_items)
        restaurant_sets = list(self.restaurant_items.values())
        
        # Count items per restaurant using Counter for efficiency
        item_restaurant_count = Counter()
        for restaurant_items in restaurant_sets:
            for item_id in restaurant_items:
                item_restaurant_count[item_id] += 1
        
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
        
        # Sort by missing percentage (descending) - only sort final results
        missing_items_analysis.sort(key=lambda x: x.missing_percentage, reverse=True)
        
        return missing_items_analysis

# Thread pool for CPU-intensive tasks
executor = ThreadPoolExecutor(max_workers=4)

def get_cache_key(filters: Dict[str, Any]) -> str:
    """Generate cache key from filters"""
    # Convert dict to sorted tuple for consistent hashing
    return str(sorted(filters.items())) if filters else "no_filters"

def is_cache_valid(cache_entry: Dict) -> bool:
    """Check if cache entry is still valid"""
    return time.time() - cache_entry["timestamp"] < CACHE_TTL

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
            processing_time=processing_time
        )
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, process_data)

@app.post("/analyze-menu", response_model=ProcessedData)
async def analyze_menu_data(api_response: Dict[str, Any]):
    """
    Analyze menu data from external API response with optimized processing.
    """
    try:
        return await process_data_async(api_response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")

@app.get("/fetch-and-analyze")
async def fetch_and_analyze(query_params: Optional[str] = None):
    """
    Fetch data from TraceVenue API and analyze it with caching.
    """
    try:
        cache_key = f"get_{query_params or 'no_params'}"
        
        with _cache_lock:
            if cache_key in _cache and is_cache_valid(_cache[cache_key]):
                cached_result = _cache[cache_key]["data"]
                cached_result.cache_hit = True
                return cached_result
        
        api_url = f"{BACKEND_BASE_URL}/{FILTERED_VARIANTS_ENDPOINT}"
        if query_params:
            api_url += f"?{query_params}"
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(api_url)
            response.raise_for_status()
            api_data = response.json()
        
        result = await process_data_async(api_data)
        
        with _cache_lock:
            _cache[cache_key] = {
                "data": result,
                "timestamp": time.time()
            }
        
        return result
        
    except httpx.RequestError as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing fetched data: {str(e)}")

@app.post("/fetch-and-analyze-with-filters")
async def fetch_and_analyze_with_filters(filters: Dict[str, Any] = None):
    """
    Fetch data from TraceVenue API with POST filters and analyze it with caching.
    """
    try:
        cache_key = get_cache_key(filters or {})
        
        with _cache_lock:
            if cache_key in _cache and is_cache_valid(_cache[cache_key]):
                cached_result = _cache[cache_key]["data"]
                cached_result.cache_hit = True
                return cached_result
        
        api_url = f"{BACKEND_BASE_URL}/{FILTERED_VARIANTS_ENDPOINT}"
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            if filters:
                response = await client.post(api_url, json=filters)
            else:
                response = await client.get(api_url)
            
            response.raise_for_status()
            api_data = response.json()
        
        result = await process_data_async(api_data)
        
        with _cache_lock:
            _cache[cache_key] = {
                "data": result,
                "timestamp": time.time()
            }
        
        return result
        
    except httpx.RequestError as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing fetched data: {str(e)}")

@app.get("/cache-stats")
async def get_cache_stats():
    """Get cache statistics"""
    with _cache_lock:
        valid_entries = sum(1 for entry in _cache.values() if is_cache_valid(entry))
        return {
            "total_cache_entries": len(_cache),
            "valid_cache_entries": valid_entries,
            "cache_ttl_seconds": CACHE_TTL
        }

@app.delete("/clear-cache")
async def clear_cache():
    """Clear all cached results"""
    with _cache_lock:
        cleared_count = len(_cache)
        _cache.clear()
    return {"message": f"Cleared {cleared_count} cache entries"}

@app.get("/restaurant-summary")
async def get_restaurant_summary():
    """This endpoint is deprecated in favor of the optimized analysis"""
    raise HTTPException(
        status_code=410, 
        detail="This endpoint is deprecated. Use /analyze-menu or /fetch-and-analyze-with-filters instead."
    )

@app.get("/")
async def root():
    return {
        "message": "Optimized Restaurant Menu Analysis API",
        "tracevenue_api": f"{BACKEND_BASE_URL}/{FILTERED_VARIANTS_ENDPOINT}",
        "performance_features": [
            "In-memory caching with TTL",
            "Async processing with thread pools",
            "Optimized set operations",
            "Single-pass data processing",
            "Counter-based calculations"
        ],
        "endpoints": {
            "POST /analyze-menu": "Analyze menu data from API response JSON (optimized)",
            "GET /fetch-and-analyze": "Fetch and analyze data from TraceVenue API with caching",
            "POST /fetch-and-analyze-with-filters": "Fetch and analyze data with POST filters and caching",
            "GET /cache-stats": "View cache statistics",
            "DELETE /clear-cache": "Clear all cached results"
        },
        "usage_examples": {
            "fetch_with_filters": "POST /fetch-and-analyze-with-filters with your Mohali payload",
            "cache_stats": "GET /cache-stats to monitor performance"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        workers=1,  
       
    )