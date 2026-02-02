# Google Maps Business Scraper

A comprehensive Google Maps scraper with tile-based search coverage and email enrichment capabilities.

## Features

- **Tile-based Search**: Divides search area into small tiles to ensure complete coverage
- **Email Enrichment**: Multiple sources for finding business emails (Hunter.io, Clearbit, website scraping)
- **Deduplication**: Prevents duplicate businesses across tiles
- **Data Export**: JSON, CSV, and Excel output formats
- **Rate Limiting**: Respects Google Maps with configurable delays
- **Proxy Support**: Rotate proxies to avoid IP blocks

## Installation

1. Clone the repository:
```bash
git clone <repo-url>
cd google-maps-scraper
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install Playwright browsers:
```bash
playwright install
```

4. Create `.env` file:
```bash
cp .env.example .env
# Edit .env and add your API keys
```

## Usage

### Web UI (Recommended)

Start the web server:
```bash
python web_server.py
```

Then open `http://localhost:5000` in your browser.

**Features:**
- 🎯 Set target number of results
- 📍 Choose from preset cities or custom bounds
- ⚡ Real-time progress tracking
- 📊 Live results display
- 💾 Export to JSON, CSV, or Excel
- ⏹️ Stop/pause anytime

### Command Line

Search by city:
```bash
python main.py -q "restaurants" -c "new_york"
```

Search by bounds:
```bash
python main.py -q "plumbers" -b "40.4774,40.9176,-74.2591,-73.7004"
```

### Options

- `-q, --query`: Search query (required)
- `-c, --city`: City name (uses predefined bounds)
- `-b, --bounds`: Bounding box as `min_lat,max_lat,min_lng,max_lng`
- `-t, --tile-size`: Tile size in degrees (default: 0.01, approx 1.1km)
- `--headless`: Run browser in headless mode
- `--no-enrich`: Skip email enrichment
- `--max-tiles`: Limit number of tiles to process

### Examples

Search with larger tiles (faster but may miss some businesses):
```bash
python main.py -q "coffee shops" -c "los_angeles" -t 0.02
```

Search without email enrichment:
```bash
python main.py -q "lawyers" -c "chicago" --no-enrich
```

Limit to specific area with small tiles:
```bash
python main.py -q "dentists" -b "40.7,40.8,-74.0,-73.9" -t 0.005
```

## Email Enrichment

The scraper uses multiple methods to find business emails:

1. **Hunter.io API**: Domain search for verified emails
2. **Clearbit API**: Company and person lookup
3. **Website Scraping**: Extracts emails from contact pages
4. **Pattern Guessing**: Generates likely emails based on business name

### API Keys

Add to your `.env` file:

```
HUNTER_API_KEY=your_hunter_api_key
CLEARBIT_API_KEY=your_clearbit_api_key
```

## Tile Size Guide

| Tile Size | Approximate Area | Use Case |
|-----------|------------------|----------|
| 0.005 | ~550m | Dense urban areas, thorough coverage |
| 0.01 | ~1.1km | Standard search (recommended) |
| 0.02 | ~2.2km | Large areas, faster but less thorough |
| 0.05 | ~5.5km | Very large areas, may miss businesses |

## Output

Results are saved in the `output/` directory:
- `businesses_latest.json` - JSON format
- `businesses_latest.csv` - CSV format
- `businesses_latest.xlsx` - Excel format
- Timestamped versions for each run

## Data Fields

- `place_id` - Google Maps unique identifier
- `name` - Business name
- `address` - Full address
- `phone` - Phone number
- `website` - Website URL
- `email` - Best email address
- `emails` - All found emails
- `rating` - Google rating (1-5)
- `review_count` - Number of reviews
- `category` - Business category
- `latitude` - Latitude coordinate
- `longitude` - Longitude coordinate
- `hours` - Operating hours
- `photos` - Photo URLs
- `description` - Business description
- `social_media` - Social media links

## Advanced Usage

### Custom City Bounds

Add to `tile_grid.py` in the `get_city_bounds` function:

```python
def get_city_bounds(city: str) -> Tuple[float, float, float, float]:
    cities = {
        # ... existing cities ...
        "my_city": (min_lat, max_lat, min_lng, max_lng),
    }
```

### Using Proxies

Add to `.env`:
```
PROXY_LIST=http://user:pass@proxy1:8080,http://user:pass@proxy2:8080
```

### Rate Limiting

Adjust in `.env`:
```
RATE_LIMIT_DELAY=2  # Seconds between requests
MAX_CONCURRENT_REQUESTS=3
```

## Notes

- Google Maps may block requests if made too quickly
- Email enrichment requires valid API keys for best results
- Tile-based approach ensures complete coverage but takes longer
- Browser must remain open during scraping (unless --headless)
- Some businesses may not have publicly available emails

## Troubleshooting

**Playwright not found**:
```bash
playwright install chromium
```

**Blocked by Google**:
- Increase RATE_LIMIT_DELAY
- Use proxies
- Try smaller tile sizes

**No emails found**:
- Check API keys in .env
- Some businesses simply don't publish emails
- Try enrich-only mode: `python main.py --enrich-only`
