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
from supabase import create_client, Client

from models import SearchConfig, Business, Tile
from tile_grid import TileGrid, get_city_bounds
from scraper import GoogleMapsScraper, ScrapingConfig
from email_enricher import EmailEnricher
from storage import BusinessStore

load_dotenv()

# Initialize Supabase client
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')
supabase: Client = None
if supabase_url and supabase_key:
    try:
        supabase = create_client(supabase_url, supabase_key)
        print("✓ Supabase connected")
    except Exception as e:
        print(f"✗ Supabase connection failed: {e}")

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


@app.route('/api/documents', methods=['GET'])
def get_saved_documents():
    """Get all saved documents from Supabase"""
    if not supabase:
        return jsonify({'error': 'Supabase not configured'}), 500
    
    try:
        response = supabase.table('saved_documents').select('*').order('created_at', desc=True).execute()
        return jsonify({'documents': response.data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/documents/<doc_id>', methods=['GET'])
def get_document(doc_id):
    """Get a specific saved document"""
    if not supabase:
        return jsonify({'error': 'Supabase not configured'}), 500
    
    try:
        response = supabase.table('saved_documents').select('*').eq('id', doc_id).single().execute()
        if response.data:
            return jsonify(response.data)
        return jsonify({'error': 'Document not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/documents/<doc_id>/download', methods=['GET'])
def download_document(doc_id):
    """Download a saved document as CSV"""
    from flask import send_file
    import io
    
    if not supabase:
        return jsonify({'error': 'Supabase not configured'}), 500
    
    try:
        response = supabase.table('saved_documents').select('*').eq('id', doc_id).single().execute()
        if not response.data:
            return jsonify({'error': 'Document not found'}), 404
        
        doc = response.data
        csv_content = doc.get('csv_content', '')
        
        return send_file(
            io.BytesIO(csv_content.encode()),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f"{doc['document_name']}_{doc_id}.csv"
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/documents/<doc_id>', methods=['DELETE'])
def delete_document(doc_id):
    """Delete a saved document"""
    if not supabase:
        return jsonify({'error': 'Supabase not configured'}), 500
    
    try:
        supabase.table('saved_documents').delete().eq('id', doc_id).execute()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@socketio.on('save_document')
def handle_save_document(data):
    """Save job results as a named document to Supabase"""
    job_id = data.get('job_id')
    document_name = data.get('document_name', f'Scrape_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
    
    job = job_manager.get_job(job_id)
    if not job or not job.businesses:
        emit('document_saved', {'error': 'No results to save'})
        return
    
    if not supabase:
        emit('document_saved', {'error': 'Supabase not configured'})
        return
    
    try:
        # Read the CSV file content
        csv_path = f"output/job_{job_id}/results.csv"
        csv_content = ""
        if os.path.exists(csv_path):
            with open(csv_path, 'r', encoding='utf-8') as f:
                csv_content = f.read()
        
        # Save to Supabase
        doc_data = {
            'job_id': job_id,
            'document_name': document_name,
            'query': job.query,
            'city': data.get('city', ''),
            'total_results': len(job.businesses),
            'businesses': job.businesses,
            'csv_content': csv_content
        }
        
        response = supabase.table('saved_documents').insert(doc_data).execute()
        
        emit('document_saved', {
            'success': True,
            'document': response.data[0] if response.data else None,
            'message': f'Document "{document_name}" saved with {len(job.businesses)} results'
        })
        
    except Exception as e:
        emit('document_saved', {'error': str(e)})


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


@socketio.on('force_email_enrichment')
def handle_force_email_enrichment(data):
    """Handle manual request to run email enrichment on existing results"""
    job_id = data.get('job_id')
    job = job_manager.get_job(job_id)
    
    if not job:
        emit('email_enrichment_error', {'job_id': job_id, 'error': 'Job not found'})
        return
    
    if not job.businesses:
        emit('email_enrichment_error', {'job_id': job_id, 'error': 'No businesses to enrich'})
        return
    
    # Create CSV writer for this job
    output_dir = f"output/job_{job_id}"
    from storage import StreamingCSVWriter
    csv_writer = StreamingCSVWriter(
        filepath=f"{output_dir}/results.csv",
        fieldnames=['place_id', 'name', 'address', 'phone', 'website', 'email', 'category', 'rating', 'review_count', 'latitude', 'longitude', 'scraped_at']
    )
    
    # Run enrichment in background thread
    def run_enrichment():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # Create a set of seen place_ids (for compatibility)
            seen_place_ids = set(b['place_id'] for b in job.businesses)
            loop.run_until_complete(run_email_enrichment(
                job_id=job_id,
                job=job,
                csv_writer=csv_writer,
                seen_place_ids=seen_place_ids,
                smart_mode=False,
                force=True  # Force update even if email exists
            ))
            emit('email_enrichment_manual_complete', {'job_id': job_id, 'message': 'Email enrichment complete!'})
        except Exception as e:
            import traceback
            print(f"[Job {job_id}] Email enrichment error: {e}")
            print(traceback.format_exc())
            emit('email_enrichment_error', {'job_id': job_id, 'error': str(e)})
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_enrichment)
    thread.daemon = True
    thread.start()
    
    emit('email_enrichment_manual_started', {'job_id': job_id, 'message': f'Starting email enrichment for {len(job.businesses)} businesses...'})


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
    
    # Calculate center point and radius for distance filtering
    center_lat = (min_lat + max_lat) / 2
    center_lng = (min_lng + max_lng) / 2
    # Calculate radius as distance from center to corner
    lat_span_km = (max_lat - min_lat) * 111
    lng_span_km = (max_lng - min_lng) * 111 * math.cos(math.radians(center_lat))
    max_radius_km = math.sqrt(lat_span_km**2 + lng_span_km**2) / 2
    
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
    tiles = tile_grid.create_grid(config, overlap=0.25)  # 25% overlap for maximum coverage
    job.tiles_total = len(tiles)
    
    socketio.emit('log_message', {'job_id': job_id, 'message': f'Search area: {area_degrees:.2f} sq degrees, Tile size: {effective_tile_size:.3f}°, Tiles: {len(tiles)}, Overlap: 25%', 'level': 'info'})
    
    # Emit job started
    socketio.emit('job_started', job.to_dict())
    print(f"[Job {job_id}] Started with {len(tiles)} tiles")
    
    # Create new event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Run async scraper in this thread's event loop
        loop.run_until_complete(scrape_worker(
            job_id, job, tiles, query, tile_grid, enrich_emails, headless, smart_mode,
            search_center=(center_lat, center_lng), max_radius_km=max_radius_km
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


async def run_email_enrichment(job_id: str, job: ScrapingJob, csv_writer, seen_place_ids: set, smart_mode: bool = False, force: bool = False):
    """Run email enrichment on all businesses in a job. Can be called during scraping or manually."""
    from email_enricher import EmailEnricher
    
    enriched_count = 0
    if not job.businesses:
        socketio.emit('log_message', {'job_id': job_id, 'message': 'No businesses to enrich', 'level': 'warning'})
        return 0
    
    socketio.emit('log_message', {'job_id': job_id, 'message': f'Starting email enrichment for {len(job.businesses)} businesses...', 'level': 'info'})
    socketio.emit('email_enrichment_started', {'job_id': job_id, 'total': len(job.businesses)})
    
    async with EmailEnricher() as enricher:
        for idx, biz_dict in enumerate(job.businesses):
            if job_manager.should_stop(job_id):
                break
            
            website = biz_dict.get('website')
            if website:
                try:
                    socketio.emit('log_message', {'job_id': job_id, 'message': f'[{idx+1}/{len(job.businesses)}] Crawling {website}...', 'level': 'debug'})
                    results = await enricher.enrich_business_from_website(website, biz_dict.get('name', ''))
                    
                    if results:
                        best_email = enricher.get_best_email(results)
                        # Update if we found a better email or if forcing (always update)
                        should_update = force or not biz_dict.get('email') or (best_email and len(best_email) > len(biz_dict.get('email', '')))
                        if best_email and should_update:
                            biz_dict['email'] = best_email
                            biz_dict['emails'] = [r.email for r in results]
                            enriched_count += 1
                            socketio.emit('log_message', {'job_id': job_id, 'message': f'  ✓ Found email: {best_email}', 'level': 'success'})
                        
                        # Update CSV with enriched email
                        csv_writer.update_row(biz_dict)
                        
                        # Emit update to frontend
                        socketio.emit('business_updated', {
                            'job_id': job_id,
                            'place_id': biz_dict['place_id'],
                            'email': best_email,
                            'emails': biz_dict.get('emails', []),
                            'progress': {'current': idx+1, 'total': len(job.businesses), 'enriched': enriched_count}
                        })
                    else:
                        socketio.emit('log_message', {'job_id': job_id, 'message': f'  No emails found', 'level': 'debug'})
                    
                    # Small delay to be nice to websites
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    socketio.emit('log_message', {'job_id': job_id, 'message': f'  Error: {str(e)[:50]}', 'level': 'error'})
            
            socketio.emit('email_enrichment_progress', {
                'job_id': job_id,
                'current': idx+1,
                'total': len(job.businesses),
                'enriched': enriched_count
            })
    
    socketio.emit('email_enrichment_completed', {
        'job_id': job_id,
        'enriched': enriched_count,
        'total': len(job.businesses)
    })
    socketio.emit('log_message', {'job_id': job_id, 'message': f'Email enrichment complete: {enriched_count}/{len(job.businesses)} businesses enriched', 'level': 'success'})
    
    return enriched_count


async def scrape_worker(job_id: str, job: ScrapingJob, tiles: list, query: str,
                       tile_grid: TileGrid, enrich_emails: bool, headless: bool, smart_mode: bool = False,
                       search_center: Tuple[float, float] = None, max_radius_km: float = None):
    """Async worker for scraping with deduplication, smart search, and radius expansion"""
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
    
    # Radius expansion settings
    center_lat, center_lng = search_center if search_center else (None, None)
    api_radius_multiplier = 1.0
    max_expansion_multiplier = 3.0  # Max 3x the original search radius
    expansion_increment = 0.5  # Increase by 50% each time
    expansion_count = 0
    max_expansions = 4  # Max 4 expansion attempts
    
    if max_radius_km:
        socketio.emit('log_message', {'job_id': job_id, 'message': f'Radius filter: {max_radius_km}km from center, max expansion: {max_expansion_multiplier}x', 'level': 'info'})
    
    empty_tile_count = 0
    # Scale max_empty_tiles with search area - larger areas need higher threshold
    max_empty_tiles = max(5, len(tiles) // 10)  # At least 5, or 10% of tiles
    socketio.emit('log_message', {'job_id': job_id, 'message': f'Empty tile threshold: {max_empty_tiles} (based on {len(tiles)} tiles)', 'level': 'debug'})
    
    socketio.emit('log_message', {'job_id': job_id, 'message': 'Initializing browser...', 'level': 'info'})
    
    try:
        async with GoogleMapsScraper(scraping_config) as scraper:
            socketio.emit('log_message', {'job_id': job_id, 'message': 'Browser ready, starting tile search...', 'level': 'success'})
            
            queries_to_try = search_queries if smart_mode else [query]
            
            # Main scraping loop with radius expansion
            while expansion_count <= max_expansions:
                if expansion_count > 0:
                    socketio.emit('log_message', {
                        'job_id': job_id, 
                        'message': f'⚡ Radius expansion #{expansion_count}: {api_radius_multiplier:.1f}x API radius ({job.current_count}/{job.target_count} found)', 
                        'level': 'warning'
                    })
                    # Reset all tiles to unsearched so we can search them again with larger radius
                    for tile in tiles:
                        tile.searched = False
                        tile.business_count = 0
                    empty_tile_count = 0
                    job.tiles_completed = 0
                
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
                            socketio.emit('log_message', {'job_id': job_id, 'message': f'  Query: "{search_query}" (expansion: {api_radius_multiplier:.1f}x)', 'level': 'debug'})
                            businesses = await scraper.search_tile(
                                tile, search_query, job_id=job_id, socketio=socketio,
                                center_lat=center_lat, center_lng=center_lng,
                                max_radius_km=max_radius_km, api_radius_multiplier=api_radius_multiplier
                            )
                            if businesses:
                                socketio.emit('log_message', {'job_id': job_id, 'message': f'  Found {len(businesses)} businesses', 'level': 'info'})
                                tile_found_businesses = True
                                
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
                            # Check if we should expand radius
                            if job.current_count < job.target_count and api_radius_multiplier < max_expansion_multiplier:
                                socketio.emit('log_message', {'job_id': job_id, 'message': f'Target not met ({job.current_count}/{job.target_count}), expanding search radius...', 'level': 'warning'})
                                break  # Break tile loop to trigger expansion
                            # Otherwise stop searching but continue to email enrichment
                            socketio.emit('log_message', {'job_id': job_id, 'message': 'Auto-stopped: No more businesses in area', 'level': 'warning'})
                            break
                    else:
                        empty_tile_count = 0
                    
                    tile_grid.mark_tile_searched(tile.id, job.current_count)
                    job.tiles_completed += 1
                    socketio.emit('progress_update', job.to_dict())
                
                # Check if target met or max expansion reached
                if job.current_count >= job.target_count:
                    break
                
                if api_radius_multiplier >= max_expansion_multiplier or expansion_count >= max_expansions:
                    socketio.emit('log_message', {'job_id': job_id, 'message': f'Max radius expansion reached ({api_radius_multiplier:.1f}x). Found {job.current_count}/{job.target_count} businesses.', 'level': 'warning'})
                    break
                
                # Expand radius for next iteration
                api_radius_multiplier += expansion_increment
                expansion_count += 1
                
                # Save current state before expansion
                socketio.emit('log_message', {'job_id': job_id, 'message': f'Expanding search: {job.current_count}/{job.target_count} found. Increasing API radius to {api_radius_multiplier:.1f}x...', 'level': 'info'})
                
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
    
    # Phase 2: Email enrichment - crawl each website individually after scraping
    enriched_count = 0
    if enrich_emails and job.businesses:
        enriched_count = await run_email_enrichment(
            job_id=job_id,
            job=job,
            csv_writer=csv_writer,
            seen_place_ids=seen_place_ids,
            smart_mode=smart_mode,
            force=False
        )
    
    socketio.emit('job_completed', {
        **job.to_dict(),
        'deduplication_stats': {
            'unique_businesses': len(seen_place_ids),
            'search_variations_used': len(queries_to_try) if smart_mode else 1
        },
        'email_enrichment': {
            'enriched': enriched_count,
            'total': len(job.businesses)
        }
    })
    socketio.emit('log_message', {'job_id': job_id, 'message': f'Completed! Found {len(seen_place_ids)} unique businesses', 'level': 'success'})


if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=8082)
