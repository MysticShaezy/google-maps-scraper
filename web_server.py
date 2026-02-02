"""
Web UI server for Google Maps Scraper
Provides REST API and real-time updates via WebSockets
"""

import asyncio
import json
import math
import os
import threading
import uuid
from datetime import datetime
from typing import Dict, Optional, Tuple, Set
from dataclasses import dataclass, asdict

import requests
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from dotenv import load_dotenv

from models import SearchConfig, Business, Tile
from tile_grid import TileGrid, get_city_bounds
from scraper import GoogleMapsScraper, ScrapingConfig
from email_enricher import EmailEnricher
from storage import BusinessStore

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Store active jobs
@dataclass
class ScrapingJob:
    id: str
    query: str
    status: str  # 'pending', 'running', 'paused', 'completed', 'error'
    target_count: int
    current_count: int
    tiles_total: int
    tiles_completed: int
    businesses: list
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    def to_dict(self):
        return {
            'id': self.id,
            'query': self.query,
            'status': self.status,
            'target_count': self.target_count,
            'current_count': self.current_count,
            'tiles_total': self.tiles_total,
            'tiles_completed': self.tiles_completed,
            'progress_percent': round((self.current_count / self.target_count) * 100, 1) if self.target_count > 0 else 0,
            'error': self.error,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None
        }


class JobManager:
    """Manages scraping jobs"""
    
    def __init__(self):
        self.jobs: Dict[str, ScrapingJob] = {}
        self.active_jobs: Dict[str, threading.Thread] = {}
        self._stop_flags: Dict[str, bool] = {}
    
    def create_job(self, query: str, target_count: int) -> ScrapingJob:
        """Create a new scraping job"""
        job_id = str(uuid.uuid4())[:8]
        job = ScrapingJob(
            id=job_id,
            query=query,
            status='pending',
            target_count=target_count,
            current_count=0,
            tiles_total=0,
            tiles_completed=0,
            businesses=[]
        )
        self.jobs[job_id] = job
        self._stop_flags[job_id] = False
        return job
    
    def get_job(self, job_id: str) -> Optional[ScrapingJob]:
        return self.jobs.get(job_id)
    
    def stop_job(self, job_id: str):
        """Signal a job to stop"""
        self._stop_flags[job_id] = True
        if job_id in self.jobs:
            self.jobs[job_id].status = 'paused'
    
    def should_stop(self, job_id: str) -> bool:
        return self._stop_flags.get(job_id, False)
    
    def delete_job(self, job_id: str):
        """Delete a job and stop if running"""
        self.stop_job(job_id)
        if job_id in self.active_jobs:
            del self.active_jobs[job_id]
        if job_id in self.jobs:
            del self.jobs[job_id]
    
    def get_all_jobs(self) -> list:
        return [job.to_dict() for job in self.jobs.values()]


job_manager = JobManager()


@app.route('/')
def index():
    """Main page"""
    return render_template('index.html')


@app.route('/api/places/autocomplete')
def places_autocomplete():
    """Google Places API autocomplete for location search"""
    query = request.args.get('q', '')
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    
    if not api_key:
        return jsonify({'error': 'GOOGLE_MAPS_API_KEY not configured'}), 500
    
    if not query or len(query) < 2:
        return jsonify({'predictions': []})
    
    try:
        url = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
        params = {
            'input': query,
            'key': api_key,
            'language': 'en',
            'components': 'country:au'  # Bias to Australia based on user location
        }
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get('status') not in ['OK', 'ZERO_RESULTS']:
            return jsonify({'error': data.get('status', 'Unknown error')}), 400
        
        predictions = []
        for place in data.get('predictions', []):
            # Extract main text and secondary text safely
            structured = place.get('structured_formatting', {})
            main_text = structured.get('main_text', place.get('description', '').split(',')[0])
            secondary_text = structured.get('secondary_text', ', '.join(place.get('description', '').split(',')[1:]))
            
            predictions.append({
                'place_id': place['place_id'],
                'description': place['description'],
                'main_text': main_text,
                'secondary_text': secondary_text
            })
        
        return jsonify({'predictions': predictions})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/places/details/<place_id>')
def place_details(place_id):
    """Get place details including coordinates"""
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    
    if not api_key:
        return jsonify({'error': 'GOOGLE_MAPS_API_KEY not configured'}), 500
    
    try:
        url = "https://maps.googleapis.com/maps/api/place/details/json"
        params = {
            'place_id': place_id,
            'key': api_key,
            'fields': 'geometry,name,formatted_address'
        }
        
        response = requests.get(url, params=params, timeout=10)
        data = response.json()
        
        if data.get('status') != 'OK':
            return jsonify({'error': data.get('status', 'Unknown error')}), 400
        
        result = data.get('result', {})
        geometry = result.get('geometry', {})
        location = geometry.get('location', {})
        viewport = geometry.get('viewport', {})
        
        return jsonify({
            'name': result.get('name'),
            'address': result.get('formatted_address'),
            'latitude': location.get('lat'),
            'longitude': location.get('lng'),
            'viewport': {
                'north': viewport.get('northeast', {}).get('lat'),
                'south': viewport.get('southwest', {}).get('lat'),
                'east': viewport.get('northeast', {}).get('lng'),
                'west': viewport.get('southwest', {}).get('lng')
            },
            'radius_meters': 10000  # Default 10km radius
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/cities')
def get_cities():
    """Get list of available cities"""
    cities = {
        'new_york': 'New York',
        'los_angeles': 'Los Angeles',
        'chicago': 'Chicago',
        'houston': 'Houston',
        'phoenix': 'Phoenix',
        'philadelphia': 'Philadelphia',
        'san_antonio': 'San Antonio',
        'san_diego': 'San Diego',
        'dallas': 'Dallas',
        'san_jose': 'San Jose'
    }
    return jsonify(cities)


@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    """Get all jobs"""
    return jsonify(job_manager.get_all_jobs())


@app.route('/api/jobs/<job_id>')
def get_job(job_id):
    """Get specific job"""
    job = job_manager.get_job(job_id)
    if job:
        return jsonify(job.to_dict())
    return jsonify({'error': 'Job not found'}), 404


@app.route('/api/jobs/<job_id>/results')
def get_job_results(job_id):
    """Get job results"""
    job = job_manager.get_job(job_id)
    if job:
        return jsonify({
            'job': job.to_dict(),
            'businesses': job.businesses
        })
    return jsonify({'error': 'Job not found'}), 404


@app.route('/api/jobs/<job_id>/export/<format>')
def export_job(job_id, format):
    """Export job results as downloadable file"""
    from flask import send_file
    import os
    
    job = job_manager.get_job(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    
    if not job.businesses:
        return jsonify({'error': 'No results to export'}), 400
    
    if format == 'csv':
        # Serve the streaming CSV file that was created during the job
        csv_path = f"output/job_{job_id}/results.csv"
        if os.path.exists(csv_path):
            return send_file(
                csv_path,
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'{job.query}_results_{job_id}.csv'
            )
        else:
            return jsonify({'error': 'CSV file not found'}), 404
    
    elif format == 'json':
        return jsonify({
            'job': job.to_dict(),
            'businesses': job.businesses
        })
    
    return jsonify({'error': 'Invalid format'}), 400


@socketio.on('start_scrape')
def handle_start_scrape(data):
    """Handle scrape start request"""
    query = data.get('query', '')
    city = data.get('city', '')
    custom_bounds = data.get('custom_bounds', '')
    target_count = int(data.get('target_count', 100))
    tile_size = float(data.get('tile_size', 0.01))
    enrich_emails = data.get('enrich_emails', True)
    headless = data.get('headless', True)
    smart_mode = data.get('smart_mode', False)
    
    # Create job
    job = job_manager.create_job(query, target_count)
    
    # Emit job created event
    emit('job_created', job.to_dict())
    
    # Start scraping in background thread
    thread = threading.Thread(
        target=run_scraper,
        args=(job.id, query, city, custom_bounds, tile_size, enrich_emails, headless, smart_mode)
    )
    thread.daemon = True
    thread.start()
    
    job_manager.active_jobs[job.id] = thread


@socketio.on('stop_scrape')
def handle_stop_scrape(data):
    """Handle scrape stop request - save results"""
    job_id = data.get('job_id')
    job = job_manager.get_job(job_id)
    if job:
        job_manager.stop_job(job_id)
        # Save current results
        if job.businesses:
            store = BusinessStore(output_dir=f"output/job_{job_id}")
            for biz_data in job.businesses:
                store.add(Business(**biz_data))
            store.save()
        emit('job_stopped', {'job_id': job_id, 'results_count': len(job.businesses)})


def run_scraper(job_id: str, query: str, city: str, custom_bounds: str, 
                tile_size: float, enrich_emails: bool, headless: bool, smart_mode: bool = False):
    """Run the scraper in a separate thread with proper SocketIO context"""
    
    job = job_manager.get_job(job_id)
    if not job:
        return
    
    # Get bounds
    if city:
        bounds = get_city_bounds(city)
    elif custom_bounds:
        parts = custom_bounds.split(',')
        bounds = tuple(map(float, parts))
    else:
        socketio.emit('job_error', {
            'job_id': job_id,
            'error': 'No search area specified'
        })
        return
    
    if not bounds:
        socketio.emit('job_error', {
            'job_id': job_id,
            'error': 'Invalid search area'
        })
        return
    
    min_lat, max_lat, min_lng, max_lng = bounds
    
    # Update job
    job.status = 'running'
    job.started_at = datetime.now()
    
    # Create tile grid with dynamic sizing to limit total tiles
    area_degrees = (max_lat - min_lat) * (max_lng - min_lng)
    max_tiles = 100  # Reasonable limit
    
    # Adjust tile size to achieve target tile count
    target_tile_size = math.sqrt(area_degrees / max_tiles)
    effective_tile_size = max(tile_size, target_tile_size, 0.05)  # Minimum 0.05° (~5.5km)
    
    config = SearchConfig(
        query=query,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
        tile_size=effective_tile_size
    )
    
    tile_grid = TileGrid(tile_size=effective_tile_size)
    tiles = tile_grid.create_grid(config, overlap=0.15)  # 15% overlap to prevent missing businesses
    job.tiles_total = len(tiles)
    
    socketio.emit('log_message', {'job_id': job_id, 'message': f'Search area: {area_degrees:.2f} sq degrees, Tile size: {effective_tile_size:.3f}°, Tiles: {len(tiles)}, Overlap: 15%', 'level': 'info'})
    
    # Emit job started
    socketio.emit('job_started', job.to_dict())
    print(f"[Job {job_id}] Started with {len(tiles)} tiles")
    
    # Create new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Run async scraper in this thread's event loop
        loop.run_until_complete(scrape_worker(
            job_id, job, tiles, query, tile_grid, enrich_emails, headless, smart_mode
        ))
    except Exception as e:
        import traceback
        print(f"[Job {job_id}] Error: {e}")
        print(traceback.format_exc())
        job.status = 'error'
        job.error = str(e)
        socketio.emit('job_error', {
            'job_id': job_id,
            'error': str(e)
        })
    finally:
        loop.close()


async def scrape_worker(job_id: str, job: ScrapingJob, tiles: list, query: str,
                       tile_grid: TileGrid, enrich_emails: bool, headless: bool, smart_mode: bool = False):
    """Async worker for scraping with deduplication and smart search"""
    from storage import StreamingCSVWriter
    
    print(f"[Job {job_id}] Starting scrape_worker with {len(tiles)} tiles")
    socketio.emit('log_message', {'job_id': job_id, 'message': f'Starting scraper with {len(tiles)} tiles...', 'level': 'info'})
    
    # Create output directory and CSV file with headers at start
    output_dir = f"output/job_{job_id}"
    os.makedirs(output_dir, exist_ok=True)
    csv_writer = StreamingCSVWriter(
        filepath=f"{output_dir}/results.csv",
        fieldnames=['place_id', 'name', 'address', 'phone', 'website', 'email', 'category', 'rating', 'review_count', 'latitude', 'longitude', 'scraped_at']
    )
    socketio.emit('log_message', {'job_id': job_id, 'message': f'Created results file: {csv_writer.get_path()}', 'level': 'info'})
    
    scraping_config = ScrapingConfig(
        headless=headless,
        delay_between_requests=float(os.getenv('RATE_LIMIT_DELAY', 0.5))
    )
    
    # Track unique businesses by place_id
    seen_place_ids: Set[str] = set()
    
    # Smart mode: generate search variations
    search_queries = [query]
    if smart_mode:
        base_terms = query.lower().split()
        variations = [
            f"{query} business",
            f"{query} company", 
            f"{query} services",
            query.replace(' ', ' and '),
        ]
        for term in base_terms:
            if term.endswith('s'):
                variations.append(query.replace(term, term[:-1]))
            else:
                variations.append(query.replace(term, term + 's'))
        search_queries = list(set([q.strip() for q in search_queries + variations if q.strip()]))
    
    socketio.emit('log_message', {'job_id': job_id, 'message': f'Will search with queries: {search_queries}', 'level': 'info'})
    
    empty_tile_count = 0
    max_empty_tiles = 5
    
    socketio.emit('log_message', {'job_id': job_id, 'message': 'Initializing browser...', 'level': 'info'})
    
    try:
        async with GoogleMapsScraper(scraping_config) as scraper:
            socketio.emit('log_message', {'job_id': job_id, 'message': 'Browser ready, starting tile search...', 'level': 'success'})
            
            async with EmailEnricher() as enricher:
                queries_to_try = search_queries if smart_mode else [query]
                
                for i, tile in enumerate(tiles):
                    if job_manager.should_stop(job_id):
                        job.status = 'paused'
                        socketio.emit('job_paused', job.to_dict())
                        socketio.emit('log_message', {'job_id': job_id, 'message': 'Job stopped by user', 'level': 'warning'})
                        return
                    
                    if job.current_count >= job.target_count:
                        socketio.emit('log_message', {'job_id': job_id, 'message': f'Target reached: {job.target_count}', 'level': 'success'})
                        break
                    
                    socketio.emit('log_message', {'job_id': job_id, 'message': f'Searching tile {i+1}/{len(tiles)} (center: {tile.center[0]:.4f},{tile.center[1]:.4f})...', 'level': 'debug'})
                    
                    tile_found_businesses = False
                    
                    for search_query in queries_to_try:
                        try:
                            socketio.emit('log_message', {'job_id': job_id, 'message': f'  Query: "{search_query}"', 'level': 'debug'})
                            businesses = await scraper.search_tile(tile, search_query, job_id=job_id, socketio=socketio)
                            
                            if businesses:
                                socketio.emit('log_message', {'job_id': job_id, 'message': f'  Found {len(businesses)} businesses', 'level': 'info'})
                                tile_found_businesses = True
                                
                                if enrich_emails:
                                    for business in businesses:
                                        if business.website and not business.email:
                                            try:
                                                results = await enricher.enrich_business(business)
                                                if results:
                                                    best_email = enricher.get_best_email(results)
                                                    business.email = best_email
                                                    business.emails = [r.email for r in results]
                                                    socketio.emit('log_message', {'job_id': job_id, 'message': f'    Enriched email for {business.name}', 'level': 'debug'})
                                            except Exception as e:
                                                print(f"Email enrichment error: {e}")
                                        
                                        if job.current_count >= job.target_count:
                                            break
                                
                                for business in businesses:
                                    if job.current_count >= job.target_count:
                                        break
                                    
                                    if business.place_id in seen_place_ids:
                                        continue
                                    
                                    if not business.name:
                                        business.name = "Business (name not extracted)"
                                    if not business.address:
                                        business.address = f"Near {business.latitude:.4f}, {business.longitude:.4f}"
                                    
                                    if smart_mode and (not business.phone or not business.website):
                                        try:
                                            await scraper.get_business_details(business)
                                        except Exception as e:
                                            print(f"Could not get details for {business.name}: {e}")
                                    
                                    seen_place_ids.add(business.place_id)
                                    
                                    biz_dict = {
                                        'place_id': business.place_id,
                                        'name': business.name,
                                        'address': business.address,
                                        'phone': business.phone,
                                        'website': business.website,
                                        'email': business.email,
                                        'emails': business.emails,
                                        'rating': business.rating,
                                        'review_count': business.review_count,
                                        'category': business.category,
                                        'latitude': business.latitude,
                                        'longitude': business.longitude,
                                        'hours': business.hours,
                                        'description': business.description,
                                        'social_media': business.social_media,
                                        'scraped_at': datetime.now().isoformat()
                                    }
                                    csv_writer.append(biz_dict)
                                    job.businesses.append(biz_dict)
                                    job.current_count += 1
                                    
                                    socketio.emit('business_found', {
                                        'job_id': job_id,
                                        'business': biz_dict,
                                        'current_count': job.current_count
                                    })
                                    socketio.emit('log_message', {'job_id': job_id, 'message': f'✓ {business.name}', 'level': 'success'})
                            else:
                                socketio.emit('log_message', {'job_id': job_id, 'message': f'  No results for "{search_query}"', 'level': 'debug'})
                        
                        except Exception as e:
                            print(f"Error searching tile {tile.id} with query '{search_query}': {e}")
                            socketio.emit('log_message', {'job_id': job_id, 'message': f'Error: {str(e)[:50]}', 'level': 'error'})
                    
                    if not tile_found_businesses:
                        empty_tile_count += 1
                        socketio.emit('log_message', {'job_id': job_id, 'message': f'Empty tile ({empty_tile_count}/{max_empty_tiles})', 'level': 'warning'})
                        if empty_tile_count >= max_empty_tiles:
                            job.status = 'completed'
                            job.completed_at = datetime.now()
                            socketio.emit('job_completed', {
                                **job.to_dict(),
                                'message': f'No more businesses found. Checked {len(queries_to_try)} search variations.',
                                'deduplication_stats': {
                                    'unique_businesses': len(seen_place_ids),
                                    'search_variations_used': len(queries_to_try)
                                }
                            })
                            socketio.emit('log_message', {'job_id': job_id, 'message': 'Auto-stopped: No more businesses in area', 'level': 'warning'})
                            return
                    else:
                        empty_tile_count = 0
                    
                    tile_grid.mark_tile_searched(tile.id, job.current_count)
                    job.tiles_completed += 1
                    socketio.emit('progress_update', job.to_dict())
    except Exception as e:
        import traceback
        print(f"[Job {job_id}] Scraper error: {e}")
        print(traceback.format_exc())
        socketio.emit('log_message', {'job_id': job_id, 'message': f'Fatal error: {str(e)}', 'level': 'error'})
        job.status = 'error'
        job.error = str(e)
        socketio.emit('job_error', {'job_id': job_id, 'error': str(e)})
        return
    
    job.status = 'completed'
    job.completed_at = datetime.now()
    socketio.emit('job_completed', {
        **job.to_dict(),
        'deduplication_stats': {
            'unique_businesses': len(seen_place_ids),
            'search_variations_used': len(queries_to_try) if smart_mode else 1
        }
    })
    socketio.emit('log_message', {'job_id': job_id, 'message': f'Completed! Found {len(seen_place_ids)} unique businesses', 'level': 'success'})


if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=8082)
