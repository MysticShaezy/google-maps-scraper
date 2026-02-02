"""
Main orchestrator for the Google Maps scraper
"""

import asyncio
import argparse
import os
from typing import List, Optional
from datetime import datetime

from dotenv import load_dotenv
from tqdm import tqdm

from models import SearchConfig, Business, Tile
from tile_grid import TileGrid, get_city_bounds
from scraper import GoogleMapsScraper, ScrapingConfig
from email_enricher import EmailEnricher
from storage import BusinessStore


class GoogleMapsScraperApp:
    """Main application for scraping Google Maps businesses"""
    
    def __init__(self):
        load_dotenv()
        self.store = BusinessStore()
        self.tile_grid: Optional[TileGrid] = None
    
    async def search_area(
        self,
        query: str,
        min_lat: float,
        max_lat: float,
        min_lng: float,
        max_lng: float,
        tile_size: float = 0.01,
        headless: bool = True,
        enrich_emails: bool = True,
        max_tiles: Optional[int] = None
    ):
        """
        Search an area using tile-based approach
        """
        print(f"\n{'='*60}")
        print(f"Starting Google Maps Scraper")
        print(f"Query: {query}")
        print(f"Area: {min_lat:.4f}, {max_lat:.4f}, {min_lng:.4f}, {max_lng:.4f}")
        print(f"Tile size: {tile_size}")
        print(f"{'='*60}\n")
        
        # Create search config
        config = SearchConfig(
            query=query,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lng=min_lng,
            max_lng=max_lng,
            tile_size=tile_size
        )
        
        # Create tile grid
        self.tile_grid = TileGrid(tile_size=tile_size)
        tiles = self.tile_grid.create_grid(config)
        
        if max_tiles:
            tiles = tiles[:max_tiles]
        
        print(f"Created {len(tiles)} tiles for search coverage")
        
        # Initialize scraper
        scraping_config = ScrapingConfig(
            headless=headless,
            delay_between_requests=float(os.getenv('RATE_LIMIT_DELAY', 2.0))
        )
        
        # Process tiles
        await self._process_tiles(tiles, query, scraping_config, enrich_emails)
        
        # Final save
        self.store.save()
        
        # Print statistics
        stats = self.store.get_statistics()
        print(f"\n{'='*60}")
        print("SCRAPING COMPLETE")
        print(f"{'='*60}")
        print(f"Total businesses: {stats['total_businesses']}")
        print(f"With website: {stats['with_website']} ({stats['website_coverage']:.1f}%)")
        print(f"With email: {stats['with_email']} ({stats['email_coverage']:.1f}%)")
        print(f"With phone: {stats['with_phone']}")
        print(f"{'='*60}\n")
    
    async def _process_tiles(
        self,
        tiles: List[Tile],
        query: str,
        config: ScrapingConfig,
        enrich_emails: bool
    ):
        """Process all tiles"""
        async with GoogleMapsScraper(config) as scraper:
            async with EmailEnricher() as enricher:
                
                with tqdm(total=len(tiles), desc="Processing tiles") as pbar:
                    for tile in tiles:
                        try:
                            # Search this tile
                            businesses = await scraper.search_tile(tile, query)
                            
                            if businesses:
                                print(f"\n  Found {len(businesses)} businesses in tile {tile.id}")
                                
                                # Enrich emails if enabled
                                if enrich_emails:
                                    for business in businesses:
                                        if business.website and not business.email:
                                            results = await enricher.enrich_business(business)
                                            if results:
                                                best_email = enricher.get_best_email(results)
                                                business.email = best_email
                                                business.emails = [r.email for r in results]
                                                print(f"    ✓ Found {len(results)} email(s) for {business.name}")
                                
                                # Add to store
                                added = self.store.add_many(businesses)
                                print(f"  Added {added} new businesses (skipped {len(businesses) - added} duplicates)")
                                
                                # Save progress periodically
                                if self.store.count % 50 == 0:
                                    self.store.save()
                                    print(f"  Progress saved: {self.store.count} total businesses")
                            
                            # Mark tile as searched
                            self.tile_grid.mark_tile_searched(tile.id, len(businesses))
                            
                        except Exception as e:
                            print(f"Error processing tile {tile.id}: {e}")
                        
                        pbar.update(1)
    
    async def enrich_existing(self):
        """Enrich emails for existing businesses without emails"""
        businesses = self.store.get_all()
        to_enrich = [b for b in businesses if b.website and not b.email]
        
        print(f"\nEnriching {len(to_enrich)} businesses without emails...")
        
        async with EmailEnricher() as enricher:
            with tqdm(total=len(to_enrich), desc="Enriching emails") as pbar:
                for business in to_enrich:
                    try:
                        results = await enricher.enrich_business(business)
                        if results:
                            best_email = enricher.get_best_email(results)
                            business.email = best_email
                            business.emails = [r.email for r in results]
                            self.store.update(business)
                            print(f"  ✓ {business.name}: {best_email}")
                    except Exception as e:
                        print(f"  ✗ Error enriching {business.name}: {e}")
                    
                    pbar.update(1)
        
        self.store.save()
        print(f"\nEnrichment complete. Total businesses with email: {self.store.get_statistics()['with_email']}")


def main():
    parser = argparse.ArgumentParser(description='Google Maps Business Scraper')
    parser.add_argument('--query', '-q', required=True, help='Search query (e.g., "restaurants in New York")')
    parser.add_argument('--city', '-c', help='City name (uses predefined bounds)')
    parser.add_argument('--bounds', '-b', help='Bounding box as min_lat,max_lat,min_lng,max_lng')
    parser.add_argument('--tile-size', '-t', type=float, default=0.01, help='Tile size in degrees (default: 0.01)')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--no-enrich', action='store_true', help='Skip email enrichment')
    parser.add_argument('--max-tiles', type=int, help='Maximum number of tiles to process')
    parser.add_argument('--enrich-only', action='store_true', help='Only enrich existing data')
    
    args = parser.parse_args()
    
    app = GoogleMapsScraperApp()
    
    if args.enrich_only:
        asyncio.run(app.enrich_existing())
        return
    
    # Determine search area
    if args.city:
        bounds = get_city_bounds(args.city)
        if not bounds:
            print(f"Unknown city: {args.city}")
            print("Use --bounds instead or add city to tile_grid.py")
            return
        min_lat, max_lat, min_lng, max_lng = bounds
    elif args.bounds:
        parts = args.bounds.split(',')
        min_lat, max_lat, min_lng, max_lng = map(float, parts)
    else:
        print("Please specify --city or --bounds")
        return
    
    # Run scraper
    asyncio.run(app.search_area(
        query=args.query,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
        tile_size=args.tile_size,
        headless=args.headless,
        enrich_emails=not args.no_enrich,
        max_tiles=args.max_tiles
    ))


if __name__ == "__main__":
    main()
