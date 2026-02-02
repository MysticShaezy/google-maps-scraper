#!/bin/bash
# Setup script for Google Maps Scraper

echo "Setting up Google Maps Scraper..."

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Install Playwright browsers
echo "Installing Playwright browsers..."
playwright install chromium

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo ""
    echo "⚠️  IMPORTANT: Edit .env and add your API keys:"
    echo "   - HUNTER_API_KEY (from hunter.io)"
    echo "   - CLEARBIT_API_KEY (from clearbit.com)"
    echo ""
fi

# Create output directory
mkdir -p output

echo ""
echo "✅ Setup complete!"
echo ""
echo "To start scraping, run:"
echo "  source venv/bin/activate"
echo "  python main.py -q 'restaurants' -c 'new_york'"
echo ""
echo "Or with custom bounds:"
echo "  python main.py -q 'plumbers' -b '40.4774,40.9176,-74.2591,-73.7004'"
echo ""
