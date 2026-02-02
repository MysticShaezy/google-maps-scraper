"""
Google Maps Business Scraper with Tile-Based Search and Email Enrichment
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Set
from datetime import datetime


@dataclass
class Business:
    """Represents a scraped business"""
    place_id: str
    name: str
    address: str
    phone: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    emails: List[str] = field(default_factory=list)
    rating: Optional[float] = None
    review_count: Optional[int] = None
    category: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    hours: Optional[Dict] = None
    photos: List[str] = field(default_factory=list)
    description: Optional[str] = None
    social_media: Dict[str, str] = field(default_factory=dict)
    scraped_at: datetime = field(default_factory=datetime.now)
    
    def __hash__(self):
        return hash(self.place_id)
    
    def __eq__(self, other):
        if isinstance(other, Business):
            return self.place_id == other.place_id
        return False


@dataclass
class Tile:
    """Represents a geographic tile for search"""
    id: str
    min_lat: float
    max_lat: float
    min_lng: float
    max_lng: float
    searched: bool = False
    business_count: int = 0
    
    @property
    def center(self) -> tuple:
        """Returns center coordinates of tile"""
        return (
            (self.min_lat + self.max_lat) / 2,
            (self.min_lng + self.max_lng) / 2
        )
    
    @property
    def bounds(self) -> tuple:
        """Returns (min_lat, max_lat, min_lng, max_lng)"""
        return (self.min_lat, self.max_lat, self.min_lng, self.max_lng)


@dataclass
class SearchConfig:
    """Configuration for search"""
    query: str
    min_lat: float
    max_lat: float
    min_lng: float
    max_lng: float
    tile_size: float = 0.01  # degrees
    keywords: List[str] = field(default_factory=list)
    category_filter: Optional[str] = None
    min_rating: Optional[float] = None
    has_website_only: bool = False
