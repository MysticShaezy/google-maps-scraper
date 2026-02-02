"""
Tile-based grid system for comprehensive Google Maps coverage
"""

import math
from typing import List, Generator, Tuple
from models import Tile, SearchConfig


class TileGrid:
    """Creates and manages geographic tiles for search coverage"""
    
    def __init__(self, tile_size: float = 0.01):
        """
        Initialize tile grid
        tile_size: size in degrees (0.01 ≈ 1.1km at equator)
        """
        self.tile_size = tile_size
        self.tiles: List[Tile] = []
        self._tile_map: dict = {}  # For quick lookup
    
    def create_grid(self, config: SearchConfig, overlap: float = 0.1) -> List[Tile]:
        """
        Create tiles covering the search area with optional overlap
        
        Args:
            config: Search configuration
            overlap: Overlap ratio (0.0-1.0), default 0.1 = 10% overlap
        """
        self.tiles = []
        tile_id = 0
        
        # Calculate step size with overlap
        step_size = self.tile_size * (1 - overlap)
        
        lat = config.min_lat
        while lat < config.max_lat:
            lng = config.min_lng
            next_lat = min(lat + self.tile_size, config.max_lat)
            
            while lng < config.max_lng:
                next_lng = min(lng + self.tile_size, config.max_lng)
                
                tile = Tile(
                    id=f"tile_{tile_id}",
                    min_lat=lat,
                    max_lat=next_lat,
                    min_lng=lng,
                    max_lng=next_lng
                )
                
                self.tiles.append(tile)
                self._tile_map[tile.id] = tile
                tile_id += 1
                
                lng = min(lng + step_size, config.max_lng)
                if lng >= config.max_lng:
                    break
            
            lat = min(lat + step_size, config.max_lat)
            if lat >= config.max_lat:
                break
        
        return self.tiles
    
    def get_tile(self, tile_id: str) -> Tile:
        """Get tile by ID"""
        return self._tile_map.get(tile_id)
    
    def get_unsearched_tiles(self) -> List[Tile]:
        """Get all tiles that haven't been searched yet"""
        return [t for t in self.tiles if not t.searched]
    
    def mark_tile_searched(self, tile_id: str, business_count: int = 0):
        """Mark a tile as searched"""
        tile = self._tile_map.get(tile_id)
        if tile:
            tile.searched = True
            tile.business_count = business_count
    
    def get_tile_for_coordinates(self, lat: float, lng: float) -> Tile:
        """Find which tile contains these coordinates"""
        for tile in self.tiles:
            if (tile.min_lat <= lat < tile.max_lat and 
                tile.min_lng <= lng < tile.max_lng):
                return tile
        return None
    
    def subdivide_tile(self, tile: Tile) -> List[Tile]:
        """
        Subdivide a tile into 4 smaller tiles
        Useful when tile returns too many results
        """
        mid_lat = (tile.min_lat + tile.max_lat) / 2
        mid_lng = (tile.min_lng + tile.max_lng) / 2
        
        new_tiles = [
            Tile(
                id=f"{tile.id}_nw",
                min_lat=mid_lat,
                max_lat=tile.max_lat,
                min_lng=tile.min_lng,
                max_lng=mid_lng
            ),
            Tile(
                id=f"{tile.id}_ne",
                min_lat=mid_lat,
                max_lat=tile.max_lat,
                min_lng=mid_lng,
                max_lng=tile.max_lng
            ),
            Tile(
                id=f"{tile.id}_sw",
                min_lat=tile.min_lat,
                max_lat=mid_lat,
                min_lng=tile.min_lng,
                max_lng=mid_lng
            ),
            Tile(
                id=f"{tile.id}_se",
                min_lat=tile.min_lat,
                max_lat=mid_lat,
                min_lng=mid_lng,
                max_lng=tile.max_lng
            )
        ]
        
        # Replace old tile with new ones
        idx = self.tiles.index(tile)
        self.tiles = self.tiles[:idx] + new_tiles + self.tiles[idx+1:]
        del self._tile_map[tile.id]
        for t in new_tiles:
            self._tile_map[t.id] = t
            
        return new_tiles
    
    @property
    def total_tiles(self) -> int:
        return len(self.tiles)
    
    @property
    def searched_tiles(self) -> int:
        return sum(1 for t in self.tiles if t.searched)
    
    @property
    def progress(self) -> float:
        if not self.tiles:
            return 0.0
        return self.searched_tiles / self.total_tiles


def calculate_tile_size_for_area(
    min_lat: float, 
    max_lat: float, 
    min_lng: float, 
    max_lng: float,
    target_tiles: int = 100
) -> float:
    """
    Calculate optimal tile size to achieve target number of tiles
    """
    lat_span = max_lat - min_lat
    lng_span = max_lng - min_lng
    area = lat_span * lng_span
    
    tile_area = area / target_tiles
    tile_size = math.sqrt(tile_area)
    
    return round(tile_size, 4)


def get_city_bounds(city: str) -> Tuple[float, float, float, float]:
    """
    Get approximate bounds for major cities
    Returns (min_lat, max_lat, min_lng, max_lng)
    """
    cities = {
        "new_york": (40.4774, 40.9176, -74.2591, -73.7004),
        "los_angeles": (33.7037, 34.3373, -118.6682, -118.1553),
        "chicago": (41.6445, 42.0230, -87.9401, -87.5241),
        "houston": (29.5370, 30.1105, -95.9136, -95.0129),
        "phoenix": (33.2903, 33.9185, -112.3237, -111.7893),
        "philadelphia": (39.8716, 40.1379, -75.2803, -74.9558),
        "san_antonio": (29.1927, 29.6281, -98.8096, -98.2208),
        "san_diego": (32.5349, 33.1146, -117.3090, -116.9085),
        "dallas": (32.6164, 33.0233, -97.0331, -96.5536),
        "san_jose": (37.1354, 37.4690, -122.0454, -121.5890),
    }
    
    city_key = city.lower().replace(" ", "_")
    return cities.get(city_key)
