#!/usr/bin/env python3
"""
Complete scraper for MacroTrends Precious Metals Data
Handles both price charts (API) and comparison/ratio charts (iframe)
"""

import requests
import re
import json
import csv
import os
import time

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.macrotrends.net/',
}

# Graph types for price charts (via API)
GRAPH_TYPES = [
    ("Historical_Chart", "INDEXMONTHLY"),
    ("Adjusted_for_Inflation", "INDEXMONTHLYINFLATION"),
    ("Annual_Percent_Change", "INDEXANNUALCHANGE"),
    ("Annual_Average_Price", "INDEXANNUALAVG"),
]

# Price charts - use API endpoint
PRICE_CHARTS = [
    ("Gold_Prices_100_Year", "1333"),
    ("Silver_Prices_100_Year", "1470"),
    ("Platinum_Prices", "2540"),
    ("Palladium_Prices", "2542"),
    ("Gold_Price_10_Years", "2627"),
]

# Comparison/Ratio charts - use iframe endpoint  
COMPARISON_CHARTS = [
    ("Gold_vs_Oil_Prices", "1334", "gold-prices-vs-oil-prices-historical-correlation"),
    ("Gold_vs_US_Dollar", "1335", "dollar-vs-gold-comparison-last-ten-years"),
    ("Dow_to_Gold_Ratio", "1378", "dow-to-gold-ratio-100-year-historical-chart"),
    ("Gold_to_Oil_Ratio", "1380", "gold-to-oil-ratio-historical-chart"),
    ("SP500_to_Gold_Ratio", "1437", "sp500-to-gold-ratio-chart"),
    ("XAU_to_Gold_Ratio", "1439", "xau-to-gold-ratio"),
    ("HUI_to_Gold_Ratio", "1440", "hui-to-gold-ratio"),
    ("Gold_to_Silver_Ratio", "1441", "gold-to-silver-ratio"),
    ("Fed_Balance_Sheet_vs_Gold", "1486", "fed-balance-sheet-vs-gold-price"),
    ("Gold_to_Monetary_Base_Ratio", "2485", "gold-to-monetary-base-ratio"),
    ("Gold_vs_Silver_Prices", "2517", "gold-prices-vs-silver-prices-historical-chart"),
    ("Platinum_vs_Gold_Prices", "2541", "platinum-prices-vs-gold-prices"),
    ("Gold_Prices_Live", "2586", "gold-prices-today-live-chart"),
    ("Silver_Prices_Live", "2589", "silver-prices-today-live-chart"),
    ("Gold_vs_Stock_Market", "2608", "gold-price-vs-stock-market-100-year-chart"),
    ("Dow_to_Silver_Ratio", "2610", "dow-to-silver-ratio-100-year-historical-chart"),
    ("Silver_to_Oil_Ratio", "2612", "silver-to-oil-ratio-historical-chart"),
]

def fetch_api_data(page_id, frequency):
    """Fetch data from MacroTrends API for price charts"""
    url = f"https://www.macrotrends.net/economic-data/{page_id}/{frequency}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get('data', [])
    except:
        return []

def fetch_iframe_data(page_id, url_slug):
    """Fetch data from iframe endpoint for comparison charts"""
    url = f"https://www.macrotrends.net/assets/php/chart_iframe_comp.php?id={page_id}&url={url_slug}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
        
        # Extract all originalData arrays
        pattern = r'var originalData\s*=\s*(\[.*?\]);'
        matches = re.findall(pattern, html, re.DOTALL)
        
        all_data = []
        for match in matches:
            try:
                data = json.loads(match)
                if data and len(data) > 0:
                    all_data.append(data)
            except json.JSONDecodeError:
                continue
        
        return all_data
    except Exception as e:
        print(f"    Error fetching iframe: {e}")
        return []

def save_to_csv(data, filepath):
    """Save data to CSV"""
    if not data:
        return False
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        if isinstance(data[0], dict):
            headers = list(data[0].keys())
            writer.writerow(headers)
            for row in data:
                writer.writerow([row.get(h, '') for h in headers])
        elif isinstance(data[0], list):
            if len(data[0]) == 2:
                writer.writerow(['date', 'value'])
            else:
                writer.writerow([f'col{i}' for i in range(len(data[0]))])
            for row in data:
                writer.writerow(row)
    
    return True

def scrape_price_chart(name, page_id, base_dir):
    """Scrape all 4 graph types for a price chart"""
    print(f"\n{'='*50}")
    print(f"Price Chart: {name}")
    print(f"{'='*50}")
    
    folder = os.path.join(base_dir, name)
    os.makedirs(folder, exist_ok=True)
    
    success_count = 0
    for graph_name, freq in GRAPH_TYPES:
        print(f"  → {graph_name}...", end=" ")
        data = fetch_api_data(page_id, freq)
        if data:
            filepath = os.path.join(folder, f"{graph_name}.csv")
            if save_to_csv(data, filepath):
                print(f"✓ {len(data)} records")
                success_count += 1
            else:
                print("✗ Save failed")
        else:
            print("✗ No data")
        time.sleep(1)
    
    return success_count

def scrape_comparison_chart(name, page_id, url_slug, base_dir):
    """Scrape comparison/ratio chart from iframe"""
    print(f"\n{'='*50}")
    print(f"Comparison Chart: {name}")
    print(f"{'='*50}")
    
    folder = os.path.join(base_dir, name)
    os.makedirs(folder, exist_ok=True)
    
    data_arrays = fetch_iframe_data(page_id, url_slug)
    
    if not data_arrays:
        print("  ✗ No data found")
        return 0
    
    success_count = 0
    for i, data in enumerate(data_arrays):
        series_name = f"Series_{i+1}" if i > 0 else "Main_Data"
        print(f"  → {series_name}...", end=" ")
        filepath = os.path.join(folder, f"{series_name}.csv")
        if save_to_csv(data, filepath):
            print(f"✓ {len(data)} records")
            success_count += 1
        else:
            print("✗ Save failed")
    
    return success_count

def main():
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "precious_metals_complete")
    os.makedirs(base_dir, exist_ok=True)
    
    print("=" * 60)
    print("MacroTrends Precious Metals - Complete Scraper")
    print("=" * 60)
    print(f"Output: {base_dir}")
    print(f"Price charts: {len(PRICE_CHARTS)}")
    print(f"Comparison charts: {len(COMPARISON_CHARTS)}")
    
    total_files = 0
    
    # Scrape price charts (4 graphs each)
    print("\n" + "=" * 60)
    print("SCRAPING PRICE CHARTS (via API)")
    print("=" * 60)
    for name, page_id in PRICE_CHARTS:
        total_files += scrape_price_chart(name, page_id, base_dir)
        time.sleep(2)
    
    # Scrape comparison charts (via iframe)
    print("\n" + "=" * 60)
    print("SCRAPING COMPARISON/RATIO CHARTS (via iframe)")
    print("=" * 60)
    for name, page_id, url_slug in COMPARISON_CHARTS:
        total_files += scrape_comparison_chart(name, page_id, url_slug, base_dir)
        time.sleep(2)
    
    print("\n" + "=" * 60)
    print("COMPLETE!")
    print(f"Total files created: {total_files}")
    print(f"Output location: {base_dir}")
    print("=" * 60)

if __name__ == "__main__":
    main()
