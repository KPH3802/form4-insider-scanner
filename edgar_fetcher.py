"""
SEC EDGAR Form 4 Filing Fetcher
"""
import requests
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from config import EDGAR_CONFIG

class EdgarFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': EDGAR_CONFIG['user_agent'],
            'Accept-Encoding': 'gzip, deflate',
        })
        self.last_request_time = 0
    
    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < EDGAR_CONFIG['request_delay']:
            time.sleep(EDGAR_CONFIG['request_delay'] - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, url):
        self._rate_limit()
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Request error for {url}: {e}")
            return None
    
    def get_recent_form4_filings(self, count=100):
        url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=only&count={count}&output=atom"
        response = self._make_request(url)
        if not response:
            return []
        filings = []
        try:
            root = ET.fromstring(response.content)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            for entry in root.findall('atom:entry', ns):
                filing = self._parse_feed_entry(entry, ns)
                if filing:
                    filings.append(filing)
        except ET.ParseError as e:
            print(f"XML parse error: {e}")
        return filings
    
    def _parse_feed_entry(self, entry, ns):
        try:
            title = entry.find('atom:title', ns)
            link = entry.find('atom:link', ns)
            updated = entry.find('atom:updated', ns)
            if title is None or link is None:
                return None
            title_text = title.text or ''
            link_href = link.get('href', '')
            parts = link_href.split('/')
            cik = None
            accession = None
            for i, part in enumerate(parts):
                if part == 'data' and i + 2 < len(parts):
                    cik = parts[i + 1]
                    accession = parts[i + 2]
                    break
            company_name = ''
            if ' - ' in title_text:
                company_name = title_text.split(' - ', 1)[1]
                if '(' in company_name:
                    company_name = company_name.split('(')[0].strip()
            return {
                'title': title_text,
                'link': link_href,
                'cik': cik,
                'accession_number': accession,
                'company_name': company_name,
                'updated': updated.text if updated is not None else None,
            }
        except Exception as e:
            print(f"Error parsing feed entry: {e}")
            return None
    
    def get_filing_index(self, cik, accession_number):
        accession_clean = accession_number.replace('-', '')
        index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/index.json"
        response = self._make_request(index_url)
        if not response:
            return None
        try:
            data = response.json()
            for item in data.get('directory', {}).get('item', []):
                name = item.get('name', '').lower()
                if name.endswith('.xml') and 'form4' not in name:
                    continue
                if name.endswith('.xml'):
                    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/{item.get('name')}"
            for item in data.get('directory', {}).get('item', []):
                name = item.get('name', '').lower()
                if name.endswith('.xml') and 'index' not in name:
                    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/{item.get('name')}"
        except Exception as e:
            print(f"Error parsing filing index: {e}")
        return None
    
    def fetch_form4_xml(self, cik, accession_number):
        xml_url = self.get_filing_index(cik, accession_number)
        if not xml_url:
            accession_clean = accession_number.replace('-', '')
            xml_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/primary_doc.xml"
        response = self._make_request(xml_url)
        if response:
            return response.text
        return None


# Convenience function for imports
def fetch_recent_filings(count=100):
    """Wrapper function to match main.py import expectations."""
    fetcher = EdgarFetcher()
    return fetcher.get_recent_form4_filings(count=count)


if __name__ == '__main__':
    fetcher = EdgarFetcher()
    print("Fetching recent Form 4 filings...")
    filings = fetcher.get_recent_form4_filings(count=10)
    print(f"Found {len(filings)} filings")
