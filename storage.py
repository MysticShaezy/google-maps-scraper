"""
Data storage and deduplication for scraped businesses
"""

import json
import csv
import os
from typing import List, Set, Dict, Optional
from datetime import datetime
from pathlib import Path

import pandas as pd

from models import Business


class StreamingCSVWriter:
    """Write businesses to a single CSV file as they come in"""
    
    def __init__(self, filepath: str, fieldnames: List[str]):
        self.filepath = Path(filepath)
        self.fieldnames = fieldnames
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self._place_id_index: Dict[str, int] = {}  # Track row positions
        self._rows: List[dict] = []  # Keep in-memory copy for updates
        
        # Create file with headers
        with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
    
    def append(self, business_dict: dict):
        """Append a single business to the CSV"""
        self._rows.append(business_dict)
        self._place_id_index[business_dict['place_id']] = len(self._rows) - 1
        
        with open(self.filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            # Only write fields that exist in our headers
            row = {k: v for k, v in business_dict.items() if k in self.fieldnames}
            writer.writerow(row)
    
    def update_row(self, business_dict: dict):
        """Update an existing row with new data (e.g., enriched email)"""
        place_id = business_dict.get('place_id')
        if place_id and place_id in self._place_id_index:
            # Update in-memory
            idx = self._place_id_index[place_id]
            self._rows[idx].update(business_dict)
            
            # Rewrite entire CSV
            with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                writer.writeheader()
                for row in self._rows:
                    writer.writerow({k: v for k, v in row.items() if k in self.fieldnames})
    
    def get_path(self) -> str:
        """Get the file path"""
        return str(self.filepath)


class BusinessStore:
    """Store for scraped business data with deduplication"""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.businesses: Dict[str, Business] = {}
        self._place_ids: Set[str] = set()
        
        # Load existing data if available
        self._load_existing()
    
    def _load_existing(self):
        """Load existing data from previous runs"""
        json_file = self.output_dir / "businesses.json"
        if json_file.exists():
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for item in data:
                        business = Business(**item)
                        self._place_ids.add(business.place_id)
                        self.businesses[business.place_id] = business
                print(f"Loaded {len(self.businesses)} existing businesses")
            except Exception as e:
                print(f"Error loading existing data: {e}")
    
    def add(self, business: Business) -> bool:
        """
        Add a business to the store
        Returns True if added, False if duplicate
        """
        if business.place_id in self._place_ids:
            return False
        
        self._place_ids.add(business.place_id)
        self.businesses[business.place_id] = business
        return True
    
    def add_many(self, businesses: List[Business]) -> int:
        """
        Add multiple businesses
        Returns count of newly added businesses
        """
        added = 0
        for business in businesses:
            if self.add(business):
                added += 1
        return added
    
    def get(self, place_id: str) -> Optional[Business]:
        """Get business by place ID"""
        return self.businesses.get(place_id)
    
    def update(self, business: Business):
        """Update existing business"""
        if business.place_id in self._place_ids:
            self.businesses[business.place_id] = business
    
    def exists(self, place_id: str) -> bool:
        """Check if business already exists"""
        return place_id in self._place_ids
    
    def get_all(self) -> List[Business]:
        """Get all businesses"""
        return list(self.businesses.values())
    
    def get_by_website(self, website: str) -> Optional[Business]:
        """Find business by website"""
        for business in self.businesses.values():
            if business.website and website in business.website:
                return business
        return None
    
    def save(self):
        """Save all data to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save as JSON
        self._save_json(timestamp)
        
        # Save as CSV
        self._save_csv(timestamp)
        
        # Save as Excel
        self._save_excel(timestamp)
        
        # Update latest files
        self._save_json("latest")
        self._save_csv("latest")
        self._save_excel("latest")
    
    def _business_to_dict(self, business: Business) -> dict:
        """Convert business to dictionary"""
        return {
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
            'hours': json.dumps(business.hours) if business.hours else None,
            'photos': json.dumps(business.photos) if business.photos else None,
            'description': business.description,
            'social_media': json.dumps(business.social_media) if business.social_media else None,
            'scraped_at': business.scraped_at.isoformat()
        }
    
    def _save_json(self, suffix: str):
        """Save as JSON"""
        data = [self._business_to_dict(b) for b in self.businesses.values()]
        file_path = self.output_dir / f"businesses_{suffix}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def _save_csv(self, suffix: str):
        """Save as CSV"""
        if not self.businesses:
            return
        
        data = [self._business_to_dict(b) for b in self.businesses.values()]
        file_path = self.output_dir / f"businesses_{suffix}.csv"
        
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
    
    def _save_excel(self, suffix: str):
        """Save as Excel"""
        if not self.businesses:
            return
        
        data = [self._business_to_dict(b) for b in self.businesses.values()]
        df = pd.DataFrame(data)
        
        file_path = self.output_dir / f"businesses_{suffix}.xlsx"
        df.to_excel(file_path, index=False, engine='openpyxl')
    
    @property
    def count(self) -> int:
        return len(self.businesses)
    
    def get_statistics(self) -> dict:
        """Get statistics about collected data"""
        businesses = list(self.businesses.values())
        
        with_website = sum(1 for b in businesses if b.website)
        with_email = sum(1 for b in businesses if b.email)
        with_phone = sum(1 for b in businesses if b.phone)
        with_rating = sum(1 for b in businesses if b.rating)
        
        return {
            'total_businesses': len(businesses),
            'with_website': with_website,
            'with_email': with_email,
            'with_phone': with_phone,
            'with_rating': with_rating,
            'website_coverage': with_website / len(businesses) * 100 if businesses else 0,
            'email_coverage': with_email / len(businesses) * 100 if businesses else 0,
        }
