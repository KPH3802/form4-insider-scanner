#!/usr/bin/env python3
"""
Download SEC Form 3/4/5 bulk data files
Uses only built-in Python libraries - no pip install needed
"""

import os
import urllib.request
import time

# Where to save files
SAVE_DIR = os.path.dirname(os.path.abspath(__file__))

# SEC requires a proper User-Agent
HEADERS = {
    'User-Agent': 'Kevin Heaney kph3802@gmail.com',
}

# Base URL for SEC bulk data
BASE_URL = "https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets"

def download_file(year, quarter):
    """Download a single quarterly file."""
    filename = f"{year}q{quarter}_form345.zip"
    url = f"{BASE_URL}/{filename}"
    filepath = os.path.join(SAVE_DIR, filename)
    
    # Skip if already downloaded and valid size
    if os.path.exists(filepath) and os.path.getsize(filepath) > 100000:
        print(f"  {filename} already exists, skipping")
        return True
    
    print(f"  Downloading {filename}...", end=" ", flush=True)
    
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=60) as response:
            content = response.read()
            
            if len(content) > 100000:
                with open(filepath, 'wb') as f:
                    f.write(content)
                size_mb = len(content) / 1024 / 1024
                print(f"OK ({size_mb:.1f} MB)")
                return True
            else:
                print(f"FAILED (too small: {len(content)} bytes)")
                return False
                
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def main():
    print(f"Saving files to: {SAVE_DIR}")
    print()
    
    # Download from 2006 Q1 to 2025 Q4
    years = range(2006, 2026)
    quarters = [1, 2, 3, 4]
    
    success = 0
    failed = 0
    
    for year in years:
        print(f"\n{year}:")
        for q in quarters:
            # SEC rate limit - be nice
            time.sleep(0.5)
            
            if download_file(year, q):
                success += 1
            else:
                failed += 1
    
    print(f"\n\nDone! Downloaded {success} files, {failed} failed.")
    print(f"Files saved to: {SAVE_DIR}")

if __name__ == "__main__":
    main()
