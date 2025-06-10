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

class StatisticsResult(BaseModel):
    price_range: Dict[str, int]  # {"min": 0, "max": 50000}
    cuisine_statistics: Dict[str, Any]  # {"unique_cuisines": [...], "total_count": 0}
    menu_statistics: Dict[str, int]  # {"total_menu_items": 0, "total_categories": 0}
    total_unique_venues: int
    service_statistics: Dict[str, Any]  # {"free_services": {...}, "paid_services": {...}}
    processing_time: float
    cache_hit: bool = False

class RestaurantStats(BaseModel):
    venue_id: str
    variants_count: int
    price_range: Dict[str, int]
    cuisine_statistics: Dict[str, Any]
    menu_statistics: Dict[str, int]
    service_statistics: Dict[str, Any]
    person_capacity: Dict[str, int]  # {"min_persons": 0, "max_persons": 0}

class RestaurantWiseResult(BaseModel):
    total_restaurants: int
    restaurants: List[RestaurantStats]
    processing_time: float
    cache_hit: bool = False

class CuisineComboStats(BaseModel):
    cuisine_combination: List[str]
    combo_string: str  # Readable combination
    variants_count: int
    restaurants_count: int
    avg_cost: float
    price_range: Dict[str, int]
    menu_items_count: int

class CuisineComboResult(BaseModel):
    total_combinations: int
    combinations: List[CuisineComboStats]
    processing_time: float
    cache_hit: bool = False

class RestaurantWiseAnalyzer:
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset analyzer state for new analysis"""
        self.restaurant_data = {}
    
    def analyze_by_restaurant(self, api_response: Dict[str, Any]) -> RestaurantWiseResult:
        """Analyze statistics separated by restaurant/venue"""
        start_time = time.time()
        variants = api_response.get("variants", [])
        
        if not variants:
            return RestaurantWiseResult(
                total_restaurants=0,
                restaurants=[],
                processing_time=0.0
            )
        
        # Group variants by venue
        venue_variants = defaultdict(list)
        for variant in variants:
            venue_id = variant.get("venueId")
            if venue_id:
                venue_variants[venue_id].append(variant)
        
        restaurants = []
        
        for venue_id, venue_variants_list in venue_variants.items():
            # Initialize data collectors for this venue
            costs = []
            cuisine_ids = []
            menu_items_count = 0
            categories = set()
            free_services = []
            paid_services = []
            min_persons_list = []
            max_persons_list = []
            
            # Process all variants for this venue
            for variant in venue_variants_list:
                # Cost data
                cost = variant.get("cost")
                if cost is not None:
                    costs.append(cost)
                
                # Person capacity
                min_persons = variant.get("minPersons")
                max_persons = variant.get("maxPersons")
                if min_persons is not None:
                    min_persons_list.append(min_persons)
                if max_persons is not None:
                    max_persons_list.append(max_persons)
                
                # Services
                free_services.extend(variant.get("freeServices", []))
                paid_services.extend(variant.get("paidServices", []))
                
                # Process menu items
                menu_items = variant.get("menuItems", [])
                menu_items_count += len(menu_items)
                
                for item in menu_items:
                    # Cuisine statistics
                    cuisines = item.get("cuisine", [])
                    cuisine_ids.extend(cuisines)
                    
                    # Category statistics with parent category logic
                    categories_list = item.get("category", [])
                    for category in categories_list:
                        parent_categories = category.get("parentCategories", [])
                        if parent_categories:
                            for parent_cat in parent_categories:
                                parent_cat_id = parent_cat.get("_id")
                                if parent_cat_id:
                                    categories.add(parent_cat_id)
                        else:
                            cat_id = category.get("_id")
                            if cat_id:
                                categories.add(cat_id)
            
            # Calculate statistics for this restaurant
            price_range = {
                "min": min(costs) if costs else 0,
                "max": max(costs) if costs else 0
            }
            
            unique_cuisines = list(set(cuisine_ids))
            cuisine_statistics = {
                "unique_cuisines": unique_cuisines,
                "unique_count": len(unique_cuisines),
                "total_count": len(cuisine_ids)
            }
            
            menu_statistics = {
                "total_menu_items": menu_items_count,
                "total_categories": len(categories)
            }
            
            unique_free_services = list(set(free_services))
            unique_paid_services = list(set(paid_services))
            
            service_statistics = {
                "free_services": {
                    "unique": unique_free_services,
                    "total_count": len(free_services)
                },
                "paid_services": {
                    "unique": unique_paid_services,
                    "total_count": len(paid_services)
                }
            }
            
            person_capacity = {
                "min_persons": min(min_persons_list) if min_persons_list else 0,
                "max_persons": max(max_persons_list) if max_persons_list else 0
            }
            
            restaurant_stats = RestaurantStats(
                venue_id=venue_id,
                variants_count=len(venue_variants_list),
                price_range=price_range,
                cuisine_statistics=cuisine_statistics,
                menu_statistics=menu_statistics,
                service_statistics=service_statistics,
                person_capacity=person_capacity
            )
            
            restaurants.append(restaurant_stats)
        
        processing_time = round(time.time() - start_time, 3)
        
        return RestaurantWiseResult(
            total_restaurants=len(restaurants),
            restaurants=restaurants,
            processing_time=processing_time
        )


class CuisineComboAnalyzer:
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset analyzer state for new analysis"""
        self.cuisine_combos = defaultdict(lambda: {
            'variants': [],
            'restaurants': set(),
            'costs': [],
            'menu_items_count': 0
        })
    
    def analyze_cuisine_combinations(self, api_response: Dict[str, Any]) -> CuisineComboResult:
        """Analyze variants by cuisine combinations"""
        start_time = time.time()
        variants = api_response.get("variants", [])
        
        if not variants:
            return CuisineComboResult(
                total_combinations=0,
                combinations=[],
                processing_time=0.0
            )
        
        # Process each variant and extract cuisine combinations
        for variant in variants:
            venue_id = variant.get("venueId")
            cost = variant.get("cost")
            
            # Extract all cuisines from this variant's menu items
            all_cuisines = set()
            menu_items = variant.get("menuItems", [])
            
            for item in menu_items:
                cuisines = item.get("cuisine", [])
                all_cuisines.update(cuisines)
            
            # Convert to sorted tuple for consistent grouping
            cuisine_combo = tuple(sorted(all_cuisines))
            
            if cuisine_combo:  # Only process if there are cuisines
                combo_data = self.cuisine_combos[cuisine_combo]
                combo_data['variants'].append(variant)
                if venue_id:
                    combo_data['restaurants'].add(venue_id)
                if cost is not None:
                    combo_data['costs'].append(cost)
                combo_data['menu_items_count'] += len(menu_items)
        
        combinations = []
        
        for cuisine_combo, data in self.cuisine_combos.items():
            cuisine_list = list(cuisine_combo)
            combo_string = " + ".join(cuisine_list) if cuisine_list else "No Cuisine"
            
            avg_cost = sum(data['costs']) / len(data['costs']) if data['costs'] else 0
            price_range = {
                "min": min(data['costs']) if data['costs'] else 0,
                "max": max(data['costs']) if data['costs'] else 0
            }
            
            combo_stats = CuisineComboStats(
                cuisine_combination=cuisine_list,
                combo_string=combo_string,
                variants_count=len(data['variants']),
                restaurants_count=len(data['restaurants']),
                avg_cost=round(avg_cost, 2),
                price_range=price_range,
                menu_items_count=data['menu_items_count']
            )
            
            combinations.append(combo_stats)
        
        combinations.sort(key=lambda x: x.variants_count, reverse=True)
        
        processing_time = round(time.time() - start_time, 3)
        
        return CuisineComboResult(
            total_combinations=len(combinations),
            combinations=combinations,
            processing_time=processing_time
        )

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

class OptimizedStatisticsAnalyzer:
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset analyzer state for new analysis"""
        self.costs = []
        self.cuisine_ids = []
        self.menu_items_count = 0
        self.categories = set()
        self.venue_ids = set()
        self.free_services = []
        self.paid_services = []
    
    def extract_and_process_statistics(self, api_response: Dict[str, Any]) -> StatisticsResult:
        """Extract comprehensive statistics from API response"""
        start_time = time.time()
        variants = api_response.get("variants", [])
        
        if not variants:
            return StatisticsResult(
                price_range={"min": 0, "max": 0},
                cuisine_statistics={"unique_cuisines": [], "total_count": 0},
                menu_statistics={"total_menu_items": 0, "total_categories": 0},
                total_unique_venues=0,
                service_statistics={"free_services": {"unique": [], "total_count": 0}, 
                                  "paid_services": {"unique": [], "total_count": 0}},
                processing_time=0.0
            )
        
        # Single pass through variants for all statistics
        for variant in variants:
            # 1. Price range extraction
            cost = variant.get("cost")
            if cost is not None:
                self.costs.append(cost)
            
            # 4. Unique venues
            venue_id = variant.get("venueId")
            if venue_id:
                self.venue_ids.add(venue_id)
            
            # 5 & 6. Services
            free_services = variant.get("freeServices", [])
            paid_services = variant.get("paidServices", [])
            
            self.free_services.extend(free_services)
            self.paid_services.extend(paid_services)
            
            # Process menu items
            menu_items = variant.get("menuItems", [])
            self.menu_items_count += len(menu_items)
            
            for item in menu_items:
                # 2. Cuisine statistics
                cuisines = item.get("cuisine", [])
                self.cuisine_ids.extend(cuisines)
                
                # 3. Category statistics (complex logic)
                categories = item.get("category", [])
                for category in categories:
                    parent_categories = category.get("parentCategories", [])
                    if parent_categories:
                        # Use parent categories instead of main category
                        for parent_cat in parent_categories:
                            parent_cat_id = parent_cat.get("_id")
                            if parent_cat_id:
                                self.categories.add(parent_cat_id)
                    else:
                        # Use main category if no parent categories
                        cat_id = category.get("_id")
                        if cat_id:
                            self.categories.add(cat_id)
        
        # Calculate final statistics
        price_range = {
            "min": min(self.costs) if self.costs else 0,
            "max": max(self.costs) if self.costs else 0
        }
        
        unique_cuisines = list(set(self.cuisine_ids))
        cuisine_statistics = {
            "unique_cuisines": unique_cuisines,
            "unique_count": len(unique_cuisines),
            "total_count": len(self.cuisine_ids)
        }
        
        menu_statistics = {
            "total_menu_items": self.menu_items_count,
            "total_categories": len(self.categories)
        }
        
        unique_free_services = list(set(self.free_services))
        unique_paid_services = list(set(self.paid_services))
        
        service_statistics = {
            "free_services": {
                "unique": unique_free_services,
                "total_count": len(self.free_services)
            },
            "paid_services": {
                "unique": unique_paid_services,
                "total_count": len(self.paid_services)
            }
        }
        
        processing_time = round(time.time() - start_time, 3)
        
        return StatisticsResult(
            price_range=price_range,
            cuisine_statistics=cuisine_statistics,
            menu_statistics=menu_statistics,
            total_unique_venues=len(self.venue_ids),
            service_statistics=service_statistics,
            processing_time=processing_time
        )

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

async def process_statistics_async(api_response: Dict[str, Any]) -> StatisticsResult:
    """Process statistics asynchronously in thread pool"""
    def process_statistics():
        analyzer = OptimizedStatisticsAnalyzer()
        return analyzer.extract_and_process_statistics(api_response)
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, process_statistics)

async def process_restaurant_wise_async(api_response: Dict[str, Any]) -> RestaurantWiseResult:
    """Process restaurant-wise statistics asynchronously in thread pool"""
    def process_restaurant_wise():
        analyzer = RestaurantWiseAnalyzer()
        return analyzer.analyze_by_restaurant(api_response)
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, process_restaurant_wise)

async def process_cuisine_combo_async(api_response: Dict[str, Any]) -> CuisineComboResult:
    """Process cuisine combination statistics asynchronously in thread pool"""
    def process_cuisine_combo():
        analyzer = CuisineComboAnalyzer()
        return analyzer.analyze_cuisine_combinations(api_response)
    
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, process_cuisine_combo)

@app.post("/analyze-menu", response_model=ProcessedData)
async def analyze_menu_data(api_response: Dict[str, Any]):
    """
    Analyze menu data from external API response with optimized processing.
    """
    try:
        return await process_data_async(api_response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing data: {str(e)}")

@app.post("/analyze-statistics", response_model=StatisticsResult)
async def analyze_statistics_data(api_response: Dict[str, Any]):
    """
    Analyze comprehensive statistics from external API response with optimized processing.
    
    Returns:
    - Price range (min/max from variant costs)
    - Cuisine statistics (unique cuisine IDs and total count)
    - Menu statistics (total menu items and categories, with parent category logic)
    - Total unique venues
    - Service statistics (unique free and paid services with counts)
    """
    try:
        return await process_statistics_async(api_response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing statistics: {str(e)}")

@app.post("/analyze-restaurant-wise", response_model=RestaurantWiseResult)
async def analyze_restaurant_wise_data(api_response: Dict[str, Any]):
    """
    Analyze statistics separated by restaurant/venue from external API response.
    
    Returns detailed statistics for each restaurant including:
    - Venue ID and variant count
    - Price range per restaurant
    - Cuisine statistics per restaurant
    - Menu statistics per restaurant
    - Service statistics per restaurant
    - Person capacity range per restaurant
    """
    try:
        return await process_restaurant_wise_async(api_response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing restaurant-wise data: {str(e)}")

@app.post("/analyze-cuisine-combinations", response_model=CuisineComboResult)
async def analyze_cuisine_combinations_data(api_response: Dict[str, Any]):
    """
    Analyze variants by cuisine combinations from external API response.
    
    Groups variants by their cuisine combinations and provides:
    - Cuisine combination arrays
    - Number of variants per combination
    - Number of restaurants offering each combination
    - Average cost and price range per combination
    - Total menu items per combination
    """
    try:
        return await process_cuisine_combo_async(api_response)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing cuisine combinations: {str(e)}")

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

@app.get("/fetch-and-analyze-restaurant-wise")
async def fetch_and_analyze_restaurant_wise(query_params: Optional[str] = None):
    """
    Fetch data from TraceVenue API and analyze restaurant-wise statistics with caching.
    """
    try:
        cache_key = f"restaurant_wise_get_{query_params or 'no_params'}"
        
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
        
        result = await process_restaurant_wise_async(api_data)
        
        with _cache_lock:
            _cache[cache_key] = {
                "data": result,
                "timestamp": time.time()
            }
        
        return result
        
    except httpx.RequestError as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing restaurant-wise data: {str(e)}")

@app.post("/fetch-and-analyze-restaurant-wise-with-filters")
async def fetch_and_analyze_restaurant_wise_with_filters(filters: Dict[str, Any] = None):
    """
    Fetch data from TraceVenue API with POST filters and analyze restaurant-wise statistics with caching.
    """
    try:
        cache_key = f"restaurant_wise_{get_cache_key(filters or {})}"
        
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
        
        result = await process_restaurant_wise_async(api_data)
        
        with _cache_lock:
            _cache[cache_key] = {
                "data": result,
                "timestamp": time.time()
            }
        
        return result
        
    except httpx.RequestError as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing restaurant-wise data: {str(e)}")

@app.get("/fetch-and-analyze-cuisine-combinations")
async def fetch_and_analyze_cuisine_combinations(query_params: Optional[str] = None):
    """
    Fetch data from TraceVenue API and analyze cuisine combinations with caching.
    """
    try:
        cache_key = f"cuisine_combo_get_{query_params or 'no_params'}"
        
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
        
        result = await process_cuisine_combo_async(api_data)
        
        with _cache_lock:
            _cache[cache_key] = {
                "data": result,
                "timestamp": time.time()
            }
        
        return result
        
    except httpx.RequestError as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing cuisine combinations: {str(e)}")

@app.post("/fetch-and-analyze-cuisine-combinations-with-filters")
async def fetch_and_analyze_cuisine_combinations_with_filters(filters: Dict[str, Any] = None):
    """
    Fetch data from TraceVenue API with POST filters and analyze cuisine combinations with caching.
    """
    try:
        cache_key = f"cuisine_combo_{get_cache_key(filters or {})}"
        
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
        
        result = await process_cuisine_combo_async(api_data)
        
        with _cache_lock:
            _cache[cache_key] = {
                "data": result,
                "timestamp": time.time()
            }
        
        return result
        
    except httpx.RequestError as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing cuisine combinations: {str(e)}")

@app.get("/fetch-and-analyze-statistics")
async def fetch_and_analyze_statistics(query_params: Optional[str] = None):
    """
    Fetch data from TraceVenue API and analyze statistics with caching.
    """
    try:
        cache_key = f"stats_get_{query_params or 'no_params'}"
        
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
        
        result = await process_statistics_async(api_data)
        
        with _cache_lock:
            _cache[cache_key] = {
                "data": result,
                "timestamp": time.time()
            }
        
        return result
        
    except httpx.RequestError as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing fetched statistics: {str(e)}")

@app.post("/fetch-and-analyze-statistics-with-filters")
async def fetch_and_analyze_statistics_with_filters(filters: Dict[str, Any] = None):
    """
    Fetch data from TraceVenue API with POST filters and analyze statistics with caching.
    """
    try:
        cache_key = f"stats_{get_cache_key(filters or {})}"
        
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
        
        result = await process_statistics_async(api_data)
        
        with _cache_lock:
            _cache[cache_key] = {
                "data": result,
                "timestamp": time.time()
            }
        
        return result
        
    except httpx.RequestError as e:
        raise HTTPException(status_code=400, detail=f"Error fetching data from TraceVenue API: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing fetched statistics: {str(e)}")

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
            "POST /analyze-statistics": "Analyze comprehensive statistics from API response JSON",
            "POST /analyze-restaurant-wise": "Analyze statistics separated by restaurant/venue",
            "POST /analyze-cuisine-combinations": "Analyze variants by cuisine combinations",
            "GET /fetch-and-analyze": "Fetch and analyze data from TraceVenue API with caching",
            "POST /fetch-and-analyze-with-filters": "Fetch and analyze data with POST filters and caching",
            "GET /fetch-and-analyze-statistics": "Fetch and analyze statistics from TraceVenue API with caching",
            "POST /fetch-and-analyze-statistics-with-filters": "Fetch and analyze statistics with POST filters and caching",
            "GET /cache-stats": "View cache statistics",
            "DELETE /clear-cache": "Clear all cached results"
        },
        "new_analysis_features": {
            "restaurant_wise_analysis": "Complete stats breakdown per restaurant including price ranges, cuisines, menu items, services, and capacity",
            "cuisine_combination_analysis": "Groups variants by cuisine combinations with variant counts, restaurant counts, pricing, and menu items"
        },
        "new_statistics_features": {
            "price_range": "Min and max cost from variants",
            "cuisine_statistics": "Unique cuisine IDs and total count",
            "menu_statistics": "Total menu items and categories (with parent category logic)",
            "total_unique_venues": "Count of unique venue IDs",
            "service_statistics": "Unique free and paid services with counts"
        },
        "usage_examples": {
            "fetch_with_filters": "POST /fetch-and-analyze-with-filters with your Mohali payload",
            "fetch_statistics": "POST /fetch-and-analyze-statistics-with-filters for comprehensive stats",
            "restaurant_analysis": "POST /analyze-restaurant-wise with your API response JSON",
            "cuisine_combo_analysis": "POST /analyze-cuisine-combinations with your API response JSON",
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