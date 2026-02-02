"""
Google Maps Scraper using Places API via HTTP requests
"""
import os
import requests
from typing import List, Optional, Dict
from dataclasses import dataclass
from models import Business, Tile


@dataclass
class ScrapingConfig:
    """Configuration for scraping"""
    headless: bool = True
    proxy: Optional[str] = None
    timeout: int = 30000
    max_retries: int = 3
    delay_between_requests: float = 2.0


class GoogleMapsScraper:
    """Scraper using Google Places API"""
    
    def __init__(self, config=None):
        self._place_cache: Dict[str, Business] = {}
        self.config = config
        self.api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        self.base_url = "https://maps.googleapis.com/maps/api/place"
    
    async def search_tile(self, tile: Tile, query: str, job_id: str = None, socketio=None) -> List[Business]:
        """Search businesses within a tile using Places API"""
        
        def log(msg, level='debug'):
            print(f"[Scraper] {msg}")
            if socketio and job_id:
                try:
                    socketio.emit('log_message', {'job_id': job_id, 'message': msg, 'level': level})
                except:
                    pass
        
        businesses = []
        center_lat, center_lng = tile.center
        
        log(f"Searching: {query} near {center_lat},{center_lng}", 'info')
        
        try:
            # Use Places API Text Search
            url = f"{self.base_url}/textsearch/json"
            params = {
                'query': f"{query} near {center_lat},{center_lng}",
                'location': f"{center_lat},{center_lng}",
                'radius': 5000,
                'key': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            result = response.json()
            
            if result.get('status') != 'OK':
                log(f"API error: {result.get('status')}", 'warning')
                return []
            
            places = result.get('results', [])
            log(f"Found {len(places)} places", 'info')
            
            for place in places:
                try:
                    location = place.get('geometry', {}).get('location', {})
                    lat = location.get('lat', center_lat)
                    lng = location.get('lng', center_lng)
                    
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
                        log(f"✓ Added: {business.name}", 'success')
                        
                except Exception as e:
                    log(f"Error: {str(e)[:50]}", 'warning')
                    continue
            
            log(f"Total: {len(businesses)} businesses", 'info')
            
        except Exception as e:
            log(f"API Error: {str(e)[:80]}", 'error')
        
        return businesses
    
    async def get_business_details(self, business: Business) -> Business:
        """Get detailed info using Place Details API"""
        
        try:
            url = f"{self.base_url}/details/json"
            params = {
                'place_id': business.place_id,
                'fields': 'formatted_phone_number,website,opening_hours',
                'key': self.api_key
            }
            
            response = requests.get(url, params=params, timeout=10)
            result = response.json()
            
            if result.get('status') == 'OK':
                details = result.get('result', {})
                business.phone = details.get('formatted_phone_number')
                business.website = details.get('website')
                
                hours = details.get('opening_hours', {}).get('weekday_text', [])
                if hours:
                    business.hours = {day: time for day, time in [h.split(': ', 1) for h in hours if ': ' in h]}
        
        except Exception as e:
            print(f"Error: {e}")
        
        return business
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
