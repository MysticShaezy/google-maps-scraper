"""
Email enrichment services for finding business emails
Supports multiple providers: Hunter.io, Clearbit, and web scraping fallback
"""

import asyncio
import re
import os
from typing import List, Optional, Dict
from dataclasses import dataclass
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup


@dataclass
class EnrichmentResult:
    """Result from email enrichment"""
    email: str
    source: str  # 'hunter', 'clearbit', 'website', 'guess'
    confidence: float
    type: Optional[str] = None  # 'personal', 'generic', etc.
    position: Optional[str] = None
    verified: bool = False


class EmailEnricher:
    """Enrich business data with email addresses"""
    
    GENERIC_PATTERNS = [
        'info@', 'contact@', 'hello@', 'support@', 
        'admin@', 'sales@', 'marketing@', 'office@',
        'general@', 'enquiries@', 'inquiries@'
    ]
    
    def __init__(self):
        self.hunter_api_key = os.getenv('HUNTER_API_KEY')
        # Clearbit removed - package is broken
        self.session: Optional[aiohttp.ClientSession] = None
        self._cache: Dict[str, List[EnrichmentResult]] = {}
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def enrich_business(self, business) -> List[EnrichmentResult]:
        """
        Find emails for a business using multiple methods
        """
        cache_key = f"{business.name}_{business.website}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        results = []
        
        # Method 1: Hunter.io API
        if self.hunter_api_key and business.website:
            hunter_results = await self._hunter_lookup(business.website)
            results.extend(hunter_results)
        
        # Method 2: Scrape website
        if business.website:
            website_results = await self._scrape_website(business.website)
            results.extend(website_results)
        
        # Method 3: Pattern-based guess
        if business.website and not results:
            guessed = await self._guess_emails(business)
            results.extend(guessed)
        
        # Deduplicate by email
        seen = set()
        unique_results = []
        for r in results:
            if r.email.lower() not in seen:
                seen.add(r.email.lower())
                unique_results.append(r)
        
        self._cache[cache_key] = unique_results
        return unique_results
    
    async def _hunter_lookup(self, domain: str) -> List[EnrichmentResult]:
        """Look up emails using Hunter.io"""
        results = []
        
        try:
            parsed = urlparse(domain)
            domain = parsed.netloc or parsed.path
            domain = domain.replace('www.', '')
            
            url = f"https://api.hunter.io/v2/domain-search"
            params = {
                'domain': domain,
                'api_key': self.hunter_api_key,
                'limit': 10
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for email in data.get('data', {}).get('emails', []):
                        results.append(EnrichmentResult(
                            email=email['value'],
                            source='hunter',
                            confidence=email.get('confidence', 50) / 100,
                            type=email.get('type'),
                            position=email.get('position'),
                            verified=email.get('verification', {}).get('status') == 'valid'
                        ))
        
        except Exception as e:
            print(f"Hunter lookup error for {domain}: {e}")
        
        return results
    
    async def _clearbit_lookup(self, domain: str) -> List[EnrichmentResult]:
        """Look up emails using Clearbit"""
        results = []
        
        try:
            parsed = urlparse(domain)
            domain = parsed.netloc or parsed.path
            domain = domain.replace('www.', '')
            
            url = f"https://company.clearbit.com/v2/combined/find"
            params = {'domain': domain}
            headers = {'Authorization': f'Bearer {self.clearbit_api_key}'}
            
            async with self.session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Extract from company data
                    company = data.get('company', {})
                    
                    # Look for email patterns in company data
                    for person in data.get('people', []):
                        if 'email' in person:
                            results.append(EnrichmentResult(
                                email=person['email'],
                                source='clearbit',
                                confidence=0.8,
                                type='personal',
                                position=person.get('employment', {}).get('role')
                            ))
        
        except Exception as e:
            print(f"Clearbit lookup error for {domain}: {e}")
        
        return results
    
    async def _scrape_website(self, url: str) -> List[EnrichmentResult]:
        """Scrape website for email addresses"""
        results = []
        
        try:
            if not url.startswith('http'):
                url = f"https://{url}"
            
            # Try main page
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    emails = self._extract_emails(html)
                    
                    for email in emails:
                        source = 'website'
                        confidence = 0.9
                        
                        # Lower confidence for generic emails
                        if any(pattern in email.lower() for pattern in self.GENERIC_PATTERNS):
                            confidence = 0.6
                        
                        results.append(EnrichmentResult(
                            email=email,
                            source=source,
                            confidence=confidence
                        ))
            
            # Try contact page
            contact_urls = [f"{url}/contact", f"{url}/contact-us", f"{url}/about"]
            for contact_url in contact_urls:
                try:
                    async with self.session.get(contact_url) as response:
                        if response.status == 200:
                            html = await response.text()
                            emails = self._extract_emails(html)
                            
                            for email in emails:
                                if not any(r.email == email for r in results):
                                    results.append(EnrichmentResult(
                                        email=email,
                                        source='website-contact',
                                        confidence=0.85
                                    ))
                except:
                    continue
        
        except Exception as e:
            print(f"Website scrape error for {url}: {e}")
        
        return results
    
    def _extract_emails(self, text: str) -> List[str]:
        """Extract email addresses from text"""
        # Pattern to match email addresses
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(pattern, text)
        
        # Filter out common false positives and example emails
        filtered = []
        for email in emails:
            email_lower = email.lower()
            if not any(x in email_lower for x in ['example.', 'test.', 'email@', 'user@']):
                filtered.append(email)
        
        return list(set(filtered))
    
    async def _guess_emails(self, business) -> List[EnrichmentResult]:
        """Generate likely email patterns based on business info"""
        results = []
        
        if not business.website:
            return results
        
        parsed = urlparse(business.website)
        domain = parsed.netloc or parsed.path
        domain = domain.replace('www.', '')
        
        # Extract name parts
        name_parts = business.name.lower().split()
        
        # Common patterns
        patterns = [
            f"info@{domain}",
            f"contact@{domain}",
            f"hello@{domain}",
            f"{name_parts[0]}@{domain}" if name_parts else None,
        ]
        
        for pattern in patterns:
            if pattern:
                results.append(EnrichmentResult(
                    email=pattern,
                    source='guess',
                    confidence=0.3
                ))
        
        return results
    
    def get_best_email(self, results: List[EnrichmentResult]) -> Optional[str]:
        """Get the best email from enrichment results"""
        if not results:
            return None
        
        # Sort by confidence, prefer verified and non-generic
        def score_result(r: EnrichmentResult):
            score = r.confidence
            if r.verified:
                score += 0.2
            if r.type == 'personal':
                score += 0.1
            if r.source == 'hunter':
                score += 0.05
            if any(pattern in r.email.lower() for pattern in self.GENERIC_PATTERNS):
                score -= 0.3
            return score
        
        sorted_results = sorted(results, key=score_result, reverse=True)
        return sorted_results[0].email if sorted_results else None
