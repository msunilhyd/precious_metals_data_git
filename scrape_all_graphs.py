#!/usr/bin/env python3
"""
Scraper for MacroTrends Precious Metals Data - All 4 Graph Types
Extracts all 4 graph variations for each chart:
1. Historical Chart (INDEXMONTHLY)
2. Adjusted for Inflation (INDEXMONTHLYINFLATION)
3. Annual % Change (INDEXANNUALCHANGE)
4. Annual Average Price (INDEXANNUALAVG)
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

# The 4 graph types available for each chart
GRAPH_TYPES = [
    ("Historical_Chart", "INDEXMONTHLY"),
    ("Adjusted_for_Inflation", "INDEXMONTHLYINFLATION"),
    ("Annual_Percent_Change", "INDEXANNUALCHANGE"),
    ("Annual_Average_Price", "INDEXANNUALAVG"),
]

# All precious metals charts - (folder_name, page_id)
CHARTS = [
    ("Gold_Prices_100_Year", "1333"),
    ("Gold_vs_Oil_Prices", "1334"),
    ("Gold_vs_US_Dollar", "1335"),
    ("Dow_to_Gold_Ratio", "1378"),
    ("Gold_to_Oil_Ratio", "1380"),
    ("SP500_to_Gold_Ratio", "1437"),
    ("XAU_to_Gold_Ratio", "1439"),
    ("HUI_to_Gold_Ratio", "1440"),
    ("Gold_to_Silver_Ratio", "1441"),
    ("Silver_Prices_100_Year", "1470"),
    ("Fed_Balance_Sheet_vs_Gold", "1486"),
    ("Gold_to_Monetary_Base_Ratio", "2485"),
    ("Gold_vs_Silver_Prices", "2517"),
    ("Platinum_Prices", "2540"),
    ("Platinum_vs_Gold_Prices", "2541"),
    ("Palladium_Prices", "2542"),
    ("Gold_Prices_Live", "2586"),
    ("Silver_Prices_Live", "2589"),
    ("Gold_vs_Stock_Market", "2608"),
    ("Dow_to_Silver_Ratio", "2610"),
    ("Silver_to_Oil_Ratio", "2612"),
    ("Gold_Price_10_Years", "2627"),
]

def fetch_api_data(page_id, frequency):
    """Fetch data from MacroTrends API endpoint"""
    api_url = f"https://www.macrotrends.net/economic-data/{page_id}/{frequency}"
    
    try:
        response = requests.get(api_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        return None

def parse_json_data(content):
    """Parse JSON response from API"""
    try:
        # Try parsing as JSON first
        data = json.loads(content)
        if 'data' in data and data['data']:
            return data['data']
    except json.JSONDecodeError:
        pass
    
    # Try extracting nested array data
    patterns = [
        r'data\s*:\s*(\[\[[\s\S]*?\]\])',
        r'"data"\s*:\s*(\[\[[\s\S]*?\]\])',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, content)
        for match in matches:
            try:
                parsed = json.loads(match.strip())
                if parsed and isinstance(parsed, list) and len(parsed) > 0:
                    return parsed
            except:
                pass
    
    return None

def save_to_csv(data, filepath):
    """Save data to CSV file"""
    if not data:
        return False
    
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
        elif isinstance(data[0], dict):
            writer.writerow(data[0].keys())
            for row in data:
                writer.writerow(row.values())
            return True
        
        for row in data:
            writer.writerow(row)
    
    return True

def scrape_chart(chart_name, page_id, base_output_dir):
    """Scrape all 4 graph types for a single chart"""
    print(f"\n{'='*50}")
    print(f"Chart: {chart_name} (page_id: {page_id})")
    print(f"{'='*50}")
    
    # Create folder for this chart
    chart_folder = os.path.join(base_output_dir, chart_name)
    os.makedirs(chart_folder, exist_ok=True)
    
    successful = 0
    
    for graph_name, frequency in GRAPH_TYPES:
        print(f"  → {graph_name}...", end=" ")
        
        content = fetch_api_data(page_id, frequency)
        
        if content:
            data = parse_json_data(content)
            if data:
                filepath = os.path.join(chart_folder, f"{graph_name}.csv")
                if save_to_csv(data, filepath):
                    print(f"✓ {len(data)} records")
                    successful += 1
                else:
                    print("✗ Save failed")
            else:
                print("✗ No data")
        else:
            print("✗ Request failed")
        
        # Small delay between requests
        time.sleep(1)
    
    return successful

def main():
    base_output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "precious_metals_charts")
    os.makedirs(base_output_dir, exist_ok=True)
    
    print("=" * 60)
    print("MacroTrends Precious Metals - All Graphs Scraper")
    print("=" * 60)
    print(f"Output directory: {base_output_dir}")
    print(f"Total charts: {len(CHARTS)}")
    print(f"Graphs per chart: {len(GRAPH_TYPES)}")
    print(f"Total files to create: {len(CHARTS) * len(GRAPH_TYPES)}")
    
    total_successful = 0
    total_failed = 0
    
    for chart_name, page_id in CHARTS:
        successful = scrape_chart(chart_name, page_id, base_output_dir)
        total_successful += successful
        total_failed += (len(GRAPH_TYPES) - successful)
        
        # Delay between charts to avoid rate limiting
        time.sleep(3)
    
    print("\n" + "=" * 60)
    print("SCRAPING COMPLETE")
    print("=" * 60)
    print(f"Total successful: {total_successful}")
    print(f"Total failed: {total_failed}")
    print(f"Output location: {base_output_dir}")
    print("=" * 60)

if __name__ == "__main__":
    main()
