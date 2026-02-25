"""
Google Maps Scraper using Places API via HTTP requests

Cost optimization notes:
- Text Search API: $32/1000 requests. Uses location+radius biasing (not redundant query text).
- Place Details API: $17/1000 (basic) + field-level charges. Only requests needed fields.
- Persistent disk cache avoids paying for the same API call twice.
- API call counter tracks usage per session for cost visibility.
"""
import os
import time
import math
import requests
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass, field
from models import Business, Tile
from api_cache import APIResponseCache


def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """
    Calculate the great circle distance in kilometers between two points
    on the earth (specified in decimal degrees)
    """
    # Convert decimal degrees to radians
    lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    return c * r


# Estimated cost per 1000 API calls (USD) - Google Maps Platform pricing
API_COSTS = {
    'text_search': 32.00,
    'place_details': 17.00,
    'autocomplete_session': 2.83,
    'autocomplete_no_session': 2.83,
}


@dataclass
class ScrapingConfig:
    """Configuration for scraping"""
    headless: bool = True
    proxy: Optional[str] = None
    timeout: int = 30000
    max_retries: int = 3
    delay_between_requests: float = 2.0


@dataclass
class APIUsageStats:
    """Track API call counts and estimated costs"""
    text_search_calls: int = 0
    text_search_cache_hits: int = 0
    place_details_calls: int = 0
    place_details_cache_hits: int = 0
    
    @property
    def estimated_cost_usd(self) -> float:
        cost = (self.text_search_calls / 1000) * API_COSTS['text_search']
        cost += (self.place_details_calls / 1000) * API_COSTS['place_details']
        return round(cost, 4)
    
    @property
    def estimated_savings_usd(self) -> float:
        saved = (self.text_search_cache_hits / 1000) * API_COSTS['text_search']
        saved += (self.place_details_cache_hits / 1000) * API_COSTS['place_details']
        return round(saved, 4)
    
    def summary(self) -> str:
        return (
            f"API Usage: {self.text_search_calls} text searches "
            f"({self.text_search_cache_hits} cache hits), "
            f"{self.place_details_calls} detail lookups "
            f"({self.place_details_cache_hits} cache hits). "
            f"Est. cost: ${self.estimated_cost_usd:.4f}, "
            f"Est. saved: ${self.estimated_savings_usd:.4f}"
        )


class GoogleMapsScraper:
    """Scraper using Google Places API with caching and cost tracking"""
    
    def __init__(self, config=None):
        self._place_cache: Dict[str, Business] = {}
        self.config = config
        self.api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        self.base_url = "https://maps.googleapis.com/maps/api/place"
        self._api_cache = APIResponseCache()
        self.usage = APIUsageStats()
    
    async def search_tile(
        self, 
        tile: Tile, 
        query: str, 
        job_id: str = None, 
        socketio = None,
        center_lat: float = None,
        center_lng: float = None,
        max_radius_km: float = None,
        api_radius_multiplier: float = 1.0
    ) -> List[Business]:
        """
        Search businesses within a tile using Places API with pagination
        Filters results to only include those within max_radius_km from center
        """
        
        def log(msg, level='debug'):
            print(f"[Scraper] {msg}")
            if socketio and job_id:
                try:
                    socketio.emit('log_message', {'job_id': job_id, 'message': msg, 'level': level})
                except:
                    pass
        
        businesses = []
        tile_center_lat, tile_center_lng = tile.center
        
        # Use provided center or tile center
        search_center_lat = center_lat if center_lat is not None else tile_center_lat
        search_center_lng = center_lng if center_lng is not None else tile_center_lng
        
        # Calculate dynamic radius based on tile size with expansion multiplier
        lat_span = tile.max_lat - tile.min_lat
        lng_span = tile.max_lng - tile.min_lng
        lat_km = lat_span * 111
        lng_km = lng_span * 111 * math.cos(math.radians(tile_center_lat))
        tile_diagonal_km = math.sqrt(lat_km**2 + lng_km**2)
        base_radius = int(tile_diagonal_km * 1000 / 2)
        search_radius = min(int(base_radius * api_radius_multiplier), 50000)  # Max 50km
        
        log(f"Searching: {query} near {tile_center_lat:.4f},{tile_center_lng:.4f} (API radius: {search_radius}m, multiplier: {api_radius_multiplier:.1f}x)", 'info')
        
        try:
            url = f"{self.base_url}/textsearch/json"
            all_places = []
            next_page_token = None
            page_count = 0
            max_pages = 3
            
            while page_count < max_pages:
                # Use location+radius for geographic biasing instead of
                # embedding "near lat,lng" in the query text. Putting location
                # info in BOTH the query string AND the location param was
                # redundant and could cause the API to return less relevant
                # results while still billing for the call.
                params = {
                    'query': query,
                    'location': f"{tile_center_lat},{tile_center_lng}",
                    'radius': search_radius,
                    'key': self.api_key
                }
                
                if next_page_token:
                    params['pagetoken'] = next_page_token
                    time.sleep(0.5)
                
                # Check disk cache first (page tokens are never cached)
                cached = None if next_page_token else self._api_cache.get(url, params)
                if cached is not None:
                    result = cached
                    self.usage.text_search_cache_hits += 1
                    log(f"Cache hit for tile search", 'debug')
                else:
                    response = requests.get(url, params=params, timeout=10)
                    result = response.json()
                    self.usage.text_search_calls += 1
                    # Cache successful responses (not pagination tokens)
                    if result.get('status') == 'OK' and not next_page_token:
                        self._api_cache.put(url, params, result)
                
                if result.get('status') != 'OK':
                    if result.get('status') == 'INVALID_REQUEST' and next_page_token:
                        log(f"Page token expired, continuing...", 'debug')
                        break
                    if page_count == 0:
                        log(f"API error: {result.get('status')}", 'warning')
                        return []
                    break
                
                places = result.get('results', [])
                all_places.extend(places)
                log(f"Page {page_count + 1}: Found {len(places)} places (total: {len(all_places)})", 'info')
                
                next_page_token = result.get('next_page_token')
                if not next_page_token:
                    break
                
                page_count += 1
                if page_count >= max_pages:
                    break
            
            log(f"Total from API: {len(all_places)} places", 'info')
            
            # Filter and process results
            filtered_count = 0
            for place in all_places:
                try:
                    location = place.get('geometry', {}).get('location', {})
                    lat = location.get('lat', tile_center_lat)
                    lng = location.get('lng', tile_center_lng)
                    
                    # Filter by distance from original center if specified
                    if max_radius_km is not None and center_lat is not None and center_lng is not None:
                        distance_km = haversine_distance(center_lat, center_lng, lat, lng)
                        if distance_km > max_radius_km:
                            filtered_count += 1
                            continue  # Skip businesses outside desired radius
                    
                    types = place.get('types', [])
                    category = types[0].replace('_', ' ').title() if types else None
                    
                    business = Business(
                        place_id=place.get('place_id', f"api_{lat}_{lng}"),
                        name=place.get('name', 'Unknown'),
                        address=place.get('formatted_address', ''),
                        phone=None,
                        website=None,
                        email=None,
                        rating=place.get('rating'),
                        review_count=place.get('user_ratings_total'),
                        category=category,
                        latitude=lat,
                        longitude=lng
                    )
                    
                    if business.place_id not in self._place_cache:
                        self._place_cache[business.place_id] = business
                        businesses.append(business)
                        
                except Exception as e:
                    log(f"Error processing place: {str(e)[:50]}", 'warning')
                    continue
            
            if filtered_count > 0:
                log(f"Filtered out {filtered_count} businesses outside {max_radius_km}km radius", 'debug')
            log(f"Total unique within radius: {len(businesses)} businesses", 'info')
            
        except Exception as e:
            log(f"API Error: {str(e)[:80]}", 'error')
        
        return businesses
    
    async def get_business_details(self, business: Business) -> Business:
        """Get detailed info using Place Details API.
        Only requests contact fields (phone, website) which are the cheapest
        detail fields. Opening hours is an Atmosphere field with higher cost
        and is only fetched when explicitly needed.
        """
        
        try:
            url = f"{self.base_url}/details/json"
            params = {
                'place_id': business.place_id,
                'fields': 'formatted_phone_number,website',
                'key': self.api_key
            }
            
            # Check cache first
            cached = self._api_cache.get(url, params)
            if cached is not None:
                result = cached
                self.usage.place_details_cache_hits += 1
            else:
                response = requests.get(url, params=params, timeout=10)
                result = response.json()
                self.usage.place_details_calls += 1
                if result.get('status') == 'OK':
                    self._api_cache.put(url, params, result)
            
            if result.get('status') == 'OK':
                details = result.get('result', {})
                business.phone = details.get('formatted_phone_number')
                business.website = details.get('website')
        
        except Exception as e:
            print(f"Error: {e}")
        
        return business
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
