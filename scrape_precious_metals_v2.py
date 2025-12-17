#!/usr/bin/env python3
"""
Scraper for MacroTrends Precious Metals Data
Uses the internal API endpoint: /economic-data/{pageID}/{frequency}
"""

import requests
import re
import json
import csv
import os
import time

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Referer': 'https://www.macrotrends.net/',
}

# Charts with (name, page_id)
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

def fetch_api_data(page_id, frequency='M'):
    """Fetch data from MacroTrends API endpoint"""
    api_url = f"https://www.macrotrends.net/economic-data/{page_id}/{frequency}"
    
    try:
        response = requests.get(api_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"  API request failed: {e}")
        return None

def parse_highcharts_data(html_content):
    """Parse Highcharts configuration from the API response"""
    data_points = []
    
    # The API returns HTML with embedded Highcharts config
    # Look for series data arrays
    patterns = [
        r'data\s*:\s*(\[\[[\s\S]*?\]\])',  # Nested array format [[date, value], ...]
        r'"data"\s*:\s*(\[\[[\s\S]*?\]\])',
        r'series\s*:\s*\[\s*\{[^}]*"data"\s*:\s*(\[\[[\s\S]*?\]\])',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, html_content)
        for match in matches:
            try:
                # Clean up the match
                clean_data = match.strip()
                # Try to parse as JSON
                parsed = json.loads(clean_data)
                if parsed and isinstance(parsed, list) and len(parsed) > 0:
                    data_points.extend(parsed)
            except json.JSONDecodeError:
                # Try fixing common JS-to-JSON issues
                try:
                    fixed = re.sub(r"'", '"', match)
                    parsed = json.loads(fixed)
                    if parsed:
                        data_points.extend(parsed)
                except:
                    pass
    
    # Also try extracting from table data if present
    table_pattern = r'<tr[^>]*>.*?<td[^>]*>([^<]+)</td>.*?<td[^>]*>\$?([\d,\.]+)</td>'
    table_matches = re.findall(table_pattern, html_content, re.DOTALL)
    if table_matches and not data_points:
        for date_str, value_str in table_matches:
            try:
                value = float(value_str.replace(',', '').replace('$', ''))
                data_points.append([date_str.strip(), value])
            except ValueError:
                continue
    
    return data_points

def save_to_csv(data, filename, output_dir):
    """Save data to CSV file"""
    if not data:
        return False
    
    filepath = os.path.join(output_dir, f"{filename}.csv")
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Determine header based on data structure
        if isinstance(data[0], list):
            if len(data[0]) == 2:
                writer.writerow(['date', 'value'])
            elif len(data[0]) == 3:
                writer.writerow(['date', 'value1', 'value2'])
            else:
                writer.writerow([f'col{i}' for i in range(len(data[0]))])
        
        for row in data:
            writer.writerow(row)
    
    print(f"  ✓ Saved {len(data)} records to {filename}.csv")
    return True

def scrape_chart(name, page_id, output_dir):
    """Scrape a single chart using the API"""
    print(f"\nProcessing: {name} (page_id: {page_id})")
    
    # Try different frequencies
    for freq in ['M', 'D', 'A']:
        print(f"  Trying frequency: {freq}")
        content = fetch_api_data(page_id, freq)
        
        if content:
            data = parse_highcharts_data(content)
            if data:
                return save_to_csv(data, name, output_dir)
        
        time.sleep(1)  # Small delay between frequency attempts
    
    print(f"  ✗ Could not extract data for {name}")
    return False

def main():
    output_dir = os.path.dirname(os.path.abspath(__file__))
    
    print("=" * 60)
    print("MacroTrends Precious Metals Data Scraper v2")
    print("=" * 60)
    print(f"Output directory: {output_dir}")
    print(f"Total charts: {len(CHARTS)}")
    
    successful = 0
    failed = 0
    
    for name, page_id in CHARTS:
        success = scrape_chart(name, page_id, output_dir)
        if success:
            successful += 1
        else:
            failed += 1
        
        # Longer delay to avoid rate limiting
        time.sleep(5)
    
    print("\n" + "=" * 60)
    print(f"Complete! Successful: {successful}, Failed: {failed}")
    print("=" * 60)

if __name__ == "__main__":
    main()
