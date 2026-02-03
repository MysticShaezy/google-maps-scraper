"""
Email enrichment services for finding business emails
Supports multiple providers: Hunter.io, Clearbit, and web scraping fallback
"""

import asyncio
import re
import os
from typing import List, Optional, Dict, Set
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin
from collections import deque

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
    page_url: Optional[str] = None  # Track which page the email was found on


class WebsiteCrawler:
    """Crawl website to find email addresses across all pages"""
    
    def __init__(self, session: aiohttp.ClientSession, max_pages: int = 20):
        self.session = session
        self.max_pages = max_pages
        self.visited: Set[str] = set()
        self.emails_found: Dict[str, EnrichmentResult] = {}
    
    async def crawl(self, start_url: str) -> List[EnrichmentResult]:
        """Crawl website starting from URL and find all emails"""
        if not start_url.startswith('http'):
            start_url = f"https://{start_url}"
        
        # Normalize domain
        parsed = urlparse(start_url)
        base_domain = parsed.netloc.replace('www.', '')
        
        # Queue for BFS crawling
        queue = deque([start_url])
        priority_urls = [
            f"{parsed.scheme}://{parsed.netloc}/contact",
            f"{parsed.scheme}://{parsed.netloc}/contact-us",
            f"{parsed.scheme}://{parsed.netloc}/about",
            f"{parsed.scheme}://{parsed.netloc}/team",
            f"{parsed.scheme}://{parsed.netloc}/staff"
        ]
        
        # Add priority URLs to front of queue
        for url in priority_urls:
            queue.appendleft(url)
        
        while queue and len(self.visited) < self.max_pages:
            url = queue.popleft()
            
            if url in self.visited:
                continue
            
            self.visited.add(url)
            
            try:
                async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as response:
                    if response.status == 200:
                        content_type = response.headers.get('content-type', '').lower()
                        if 'text/html' not in content_type:
                            continue
                        
                        html = await response.text()
                        await self._process_page(url, html, base_domain, queue)
                        
            except Exception as e:
                print(f"Error crawling {url}: {e}")
                continue
        
        return list(self.emails_found.values())
    
    async def _process_page(self, url: str, html: str, base_domain: str, queue: deque):
        """Process a single page - extract emails and find links"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract emails from this page
        emails = self._extract_emails_from_text(html)
        
        for email in emails:
            email_lower = email.lower()
            if email_lower not in self.emails_found:
                # Determine confidence based on email type and page
                confidence = self._calculate_confidence(email, url)
                
                self.emails_found[email_lower] = EnrichmentResult(
                    email=email,
                    source='website-crawl',
                    confidence=confidence,
                    page_url=url
                )
        
        # Find more links to crawl (only same domain)
        if len(self.visited) < self.max_pages:
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(url, href)
                parsed_link = urlparse(full_url)
                
                # Only follow links on same domain
                if parsed_link.netloc.replace('www.', '') == base_domain:
                    # Skip common non-content URLs
                    if not any(x in full_url.lower() for x in ['.pdf', '.jpg', '.png', '.gif', '.css', '.js', '?', '#', 'tel:', 'mailto:', 'javascript:']):
                        if full_url not in self.visited:
                            queue.append(full_url)
    
    def _extract_emails_from_text(self, text: str) -> List[str]:
        """Extract email addresses from text with improved pattern"""
        # Pattern to match email addresses - handles common obfuscations
        patterns = [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            r'[A-Za-z0-9._%+-]+\s*@\s*[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}',  # with spaces
            r'[A-Za-z0-9._%+-]+\[at\][A-Za-z0-9.-]+\.[A-Z|a-z]{2,}',  # [at] obfuscation
            r'[A-Za-z0-9._%+-]+\(at\)[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}',  # (at) obfuscation
        ]
        
        emails = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            emails.extend(matches)
        
        # Clean up obfuscated emails
        cleaned = []
        for email in emails:
            email = email.replace('[at]', '@').replace('(at)', '@').replace(' ', '').strip()
            if re.match(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', email):
                cleaned.append(email)
        
        # Filter out common false positives
        filtered = []
        for email in cleaned:
            email_lower = email.lower()
            if not any(x in email_lower for x in ['example.', 'test.', 'email@', 'user@', 'name@', 'yourname@', 'firstname@']):
                filtered.append(email)
        
        return list(set(filtered))
    
    def _calculate_confidence(self, email: str, page_url: str) -> float:
        """Calculate confidence score for an email found on a page"""
        email_lower = email.lower()
        url_lower = page_url.lower()
        
        base_confidence = 0.85
        
        # Boost for contact/about pages
        if any(x in url_lower for x in ['contact', 'about', 'team', 'staff']):
            base_confidence += 0.1
        
        # Reduce for generic emails
        generic_patterns = ['info@', 'contact@', 'hello@', 'support@', 'admin@', 'sales@']
        if any(pattern in email_lower for pattern in generic_patterns):
            base_confidence -= 0.2
        
        # Boost for personal-looking emails (first.last patterns)
        if re.match(r'^[a-z]+\.[a-z]+@', email_lower):
            base_confidence += 0.05
        
        return min(base_confidence, 1.0)


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
    
    async def enrich_business_from_website(self, website: str, business_name: str = "") -> List[EnrichmentResult]:
        """
        Enrich a business by crawling their entire website for email addresses.
        This is called sequentially after Google scraping completes.
        """
        if not website:
            return []
        
        # Use the WebsiteCrawler for deep crawling
        crawler = WebsiteCrawler(self.session, max_pages=15)
        return await crawler.crawl(website)
    
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
        """Scrape website using deep crawler to find emails across all pages"""
        crawler = WebsiteCrawler(self.session, max_pages=15)
        return await crawler.crawl(url)
    
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
