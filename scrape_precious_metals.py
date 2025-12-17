#!/usr/bin/env python3
"""
Scraper for MacroTrends Precious Metals Data
Extracts historical data from all precious metals charts and saves to CSV files.
Uses the internal API endpoint: /economic-data/{pageID}/{frequency}
"""

import requests
import re
import json
import csv
import os
import time
from urllib.parse import urlparse

# Headers to mimic a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Referer': 'https://www.macrotrends.net/',
}

# All precious metals charts - (name, page_id) extracted from URLs
# URL pattern: https://www.macrotrends.net/{page_id}/...
CHARTS = [
    ("Gold_Prices_100_Year_Historical", "1333"),
    ("Gold_Prices_vs_Oil_Prices", "1334"),
    ("Gold_Prices_US_Dollar_Correlation", "1335"),
    ("Dow_to_Gold_Ratio", "1378"),
    ("Gold_to_Oil_Ratio", "1380"),
    ("SP500_to_Gold_Ratio", "1437"),
    ("XAU_to_Gold_Ratio", "1439"),
    ("HUI_to_Gold_Ratio", "1440"),
    ("Gold_to_Silver_Ratio", "1441"),
    ("Silver_Prices_100_Year_Historical", "1470"),
    ("Fed_Balance_Sheet_vs_Gold_Price", "1486"),
    ("Gold_to_Monetary_Base_Ratio", "2485"),
    ("Gold_Prices_vs_Silver_Prices", "2517"),
    ("Platinum_Prices_Historical", "2540"),
    ("Platinum_Prices_vs_Gold_Prices", "2541"),
    ("Palladium_Prices_Historical", "2542"),
    ("Gold_Prices_Today_Live", "2586"),
    ("Silver_Prices_Today_Live", "2589"),
    ("Gold_Price_vs_Stock_Market", "2608"),
    ("Dow_to_Silver_Ratio", "2610"),
    ("Silver_to_Oil_Ratio", "2612"),
    ("Gold_Price_Last_10_Years", "2627"),
]

# Frequency options: 'D' (daily), 'W' (weekly), 'M' (monthly), 'Q' (quarterly), 'A' (annual)
DEFAULT_FREQUENCY = 'M'

def extract_chart_data(html_content):
    """
    Extract chart data from MacroTrends page HTML.
    MacroTrends embeds data in JavaScript arrays within the page.
    """
    data_points = []
    
    # Pattern 1: Look for chartData variable
    pattern1 = r'var\s+chartData\s*=\s*(\[[\s\S]*?\]);'
    match = re.search(pattern1, html_content)
    if match:
        try:
            data = json.loads(match.group(1))
            return data
        except json.JSONDecodeError:
            pass
    
    # Pattern 2: Look for data in Highcharts series format
    pattern2 = r'series\s*:\s*\[\s*\{[^}]*data\s*:\s*(\[[\s\S]*?\])\s*[,}]'
    matches = re.findall(pattern2, html_content)
    for match in matches:
        try:
            # Clean up the data string
            clean_match = re.sub(r'\s+', ' ', match)
            data = json.loads(clean_match)
            if data and len(data) > 0:
                data_points.extend(data)
        except json.JSONDecodeError:
            pass
    
    # Pattern 3: Look for data arrays with date and value
    pattern3 = r'\[\s*"(\d{4}-\d{2}-\d{2})"\s*,\s*([\d.]+)\s*\]'
    matches = re.findall(pattern3, html_content)
    if matches:
        data_points = [{"date": m[0], "value": float(m[1])} for m in matches]
    
    # Pattern 4: Look for dataTable or similar structures
    pattern4 = r'var\s+dataTable\s*=\s*(\[[\s\S]*?\]);'
    match = re.search(pattern4, html_content)
    if match:
        try:
            data = json.loads(match.group(1))
            return data
        except json.JSONDecodeError:
            pass
    
    # Pattern 5: Extract from script tags containing chart configuration
    pattern5 = r'originalData\s*=\s*(\[[\s\S]*?\]);'
    match = re.search(pattern5, html_content)
    if match:
        try:
            # Parse the JavaScript array
            raw_data = match.group(1)
            # Convert JavaScript object notation to JSON
            raw_data = re.sub(r"'", '"', raw_data)
            data = json.loads(raw_data)
            return data
        except json.JSONDecodeError:
            pass
    
    # Pattern 6: Look for inline data in table format
    table_pattern = r'<tr[^>]*>\s*<td[^>]*>(\d{4}-\d{2}-\d{2})</td>\s*<td[^>]*>\$([\d,\.]+)</td>'
    table_matches = re.findall(table_pattern, html_content)
    if table_matches:
        data_points = [{"date": m[0], "value": float(m[1].replace(',', ''))} for m in table_matches]
    
    return data_points

def extract_data_from_script(html_content):
    """
    Alternative extraction method - looks for data in various script formats
    """
    results = []
    
    # Find all script tags
    script_pattern = r'<script[^>]*>([\s\S]*?)</script>'
    scripts = re.findall(script_pattern, html_content)
    
    for script in scripts:
        # Look for date-value pairs
        date_value_pattern = r'\["(\d{4}-\d{2}-\d{2})"\s*,\s*([\d.]+(?:e[+-]?\d+)?)\s*(?:,\s*"[^"]*")?\]'
        matches = re.findall(date_value_pattern, script)
        if matches:
            for m in matches:
                try:
                    results.append({
                        "date": m[0],
                        "value": float(m[1])
                    })
                except ValueError:
                    continue
    
    # Remove duplicates while preserving order
    seen = set()
    unique_results = []
    for item in results:
        key = (item['date'], item['value'])
        if key not in seen:
            seen.add(key)
            unique_results.append(item)
    
    return unique_results

def save_to_csv(data, filename, output_dir):
    """Save extracted data to CSV file"""
    if not data:
        print(f"  No data to save for {filename}")
        return False
    
    filepath = os.path.join(output_dir, f"{filename}.csv")
    
    # Determine columns based on data structure
    if isinstance(data[0], dict):
        columns = list(data[0].keys())
    elif isinstance(data[0], list):
        # Assume [date, value] or [date, value1, value2, ...]
        if len(data[0]) == 2:
            columns = ['date', 'value']
        elif len(data[0]) == 3:
            columns = ['date', 'value1', 'value2']
        else:
            columns = [f'col{i}' for i in range(len(data[0]))]
        # Convert list data to dict
        data = [dict(zip(columns, row)) for row in data]
    else:
        print(f"  Unknown data format for {filename}")
        return False
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"  ✓ Saved {len(data)} records to {filename}.csv")
    return True

def scrape_chart(name, url, output_dir):
    """Scrape a single chart page and extract data"""
    print(f"\nProcessing: {name}")
    print(f"  URL: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        html = response.text
        
        # Try multiple extraction methods
        data = extract_chart_data(html)
        
        if not data:
            data = extract_data_from_script(html)
        
        if data:
            save_to_csv(data, name, output_dir)
            return True
        else:
            print(f"  ✗ Could not extract data from {name}")
            # Save raw HTML for debugging
            debug_path = os.path.join(output_dir, f"_debug_{name}.html")
            with open(debug_path, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"  Saved HTML for debugging: _debug_{name}.html")
            return False
            
    except requests.RequestException as e:
        print(f"  ✗ Request failed: {e}")
        return False
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False

def main():
    """Main function to scrape all precious metals charts"""
    output_dir = os.path.dirname(os.path.abspath(__file__))
    
    print("=" * 60)
    print("MacroTrends Precious Metals Data Scraper")
    print("=" * 60)
    print(f"Output directory: {output_dir}")
    print(f"Total charts to scrape: {len(CHART_URLS)}")
    
    successful = 0
    failed = 0
    
    for name, url in CHART_URLS:
        success = scrape_chart(name, url, output_dir)
        if success:
            successful += 1
        else:
            failed += 1
        
        # Be nice to the server
        time.sleep(2)
    
    print("\n" + "=" * 60)
    print(f"Scraping complete!")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print("=" * 60)

if __name__ == "__main__":
    main()
