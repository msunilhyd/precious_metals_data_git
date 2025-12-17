#!/usr/bin/env python3
"""
MacroTrends Complete Data Scraper
Scrapes all sections: Market Indexes, Precious Metals, Energy, Commodities,
Exchange Rates, Interest Rates, and Economy.

Organizes data into folders by section with proper date formatting.
"""

import requests
import re
import json
import csv
import os
import time
from datetime import datetime

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.macrotrends.net/',
}

# Graph types for price charts (via API)
GRAPH_TYPES = [
    ("01_historical", "INDEXMONTHLY"),
    ("02_inflation_adjusted", "INDEXMONTHLYINFLATION"),
    ("03_annual_change", "INDEXANNUALCHANGE"),
    ("04_annual_average", "INDEXANNUALAVG"),
]

# ============================================================================
# ALL SECTIONS AND CHARTS
# ============================================================================

SECTIONS = {
    "01_market_indexes": [
        ("stock_market_secular_cycles", "1296"),
        ("dow_jones_100_year", "1319"),
        ("nasdaq_45_year", "1320"),
        ("sp500_earnings_history", "1324"),
        ("dow_jones_10_year_daily", "1358"),
        ("sp500_90_year", "2324"),
        ("stock_market_by_president", "2481"),
        ("sp500_by_president", "2482"),
        ("dow_jones_1929_bear_market", "2484"),
        ("sp500_10_year_daily", "2488"),
        ("nasdaq_10_year_daily", "2489"),
        ("sp500_ytd_performance", "2490"),
        ("dow_jones_ytd_performance", "2505"),
        ("sp500_annual_returns", "2526"),
        ("nasdaq_ytd_performance", "2527"),
        ("dow_vs_nasdaq", "2528"),
        ("dow_to_gdp_ratio", "2574"),
        ("sp500_pe_ratio", "2577"),
        ("shanghai_composite_china", "2592"),
        ("nikkei_225_japan", "2593"),
        ("hang_seng_hong_kong", "2594"),
        ("dax_30_germany", "2595"),
        ("cac_40_france", "2596"),
        ("bovespa_brazil", "2597"),
        ("nasdaq_to_dow_ratio", "2600"),
        ("sp500_vs_durable_goods", "2601"),
        ("vix_volatility_index", "2603"),
        ("stock_market_by_president_election", "2613"),
        ("sp500_by_president_election", "2614"),
        ("trump_stock_market", "2616"),
        ("dow_jones_by_year", "2622"),
        ("nasdaq_by_year", "2623"),
    ],
    
    "02_precious_metals": [
        ("gold_prices_100_year", "1333"),
        ("gold_vs_oil_prices", "1334"),
        ("gold_vs_us_dollar", "1335"),
        ("dow_to_gold_ratio", "1378"),
        ("gold_to_oil_ratio", "1380"),
        ("sp500_to_gold_ratio", "1437"),
        ("xau_to_gold_ratio", "1439"),
        ("hui_to_gold_ratio", "1440"),
        ("gold_to_silver_ratio", "1441"),
        ("silver_prices_100_year", "1470"),
        ("fed_balance_sheet_vs_gold", "1486"),
        ("gold_to_monetary_base_ratio", "2485"),
        ("gold_vs_silver_prices", "2517"),
        ("platinum_prices", "2540"),
        ("platinum_vs_gold_prices", "2541"),
        ("palladium_prices", "2542"),
        ("gold_prices_live", "2586"),
        ("silver_prices_live", "2589"),
        ("gold_vs_stock_market", "2608"),
        ("dow_to_silver_ratio", "2610"),
        ("silver_to_oil_ratio", "2612"),
        ("gold_price_10_years", "2627"),
    ],
    
    "03_energy": [
        ("crude_oil_prices_70_year", "1369"),
        ("crude_oil_vs_sp500", "1453"),
        ("natural_gas_prices", "2478"),
        ("heating_oil_prices", "2479"),
        ("brent_crude_oil_10_year", "2480"),
        ("oil_vs_natural_gas", "2500"),
        ("oil_vs_gasoline_prices", "2501"),
        ("oil_vs_propane_prices", "2502"),
        ("wti_crude_oil_10_year", "2516"),
        ("us_crude_oil_production", "2562"),
        ("us_crude_oil_exports", "2563"),
        ("saudi_arabia_crude_production", "2564"),
        ("us_crude_oil_reserves", "2565"),
        ("crude_oil_prices_live", "2566"),
    ],
    
    "04_commodities": [
        ("copper_prices", "1476"),
        ("soybean_prices", "2531"),
        ("corn_prices", "2532"),
        ("cotton_prices", "2533"),
        ("wheat_prices", "2534"),
        ("coffee_prices", "2535"),
        ("oats_prices", "2536"),
        ("sugar_prices", "2537"),
        ("soybean_oil_prices", "2538"),
        ("copper_prices_live", "2590"),
        ("lumber_prices", "2637"),
    ],
    
    "05_exchange_rates": [
        ("us_dollar_index", "1329"),
        ("euro_dollar", "2548"),
        ("pound_dollar", "2549"),
        ("dollar_yen", "2550"),
        ("aud_usd", "2551"),
        ("euro_swiss_franc", "2552"),
        ("euro_pound", "2553"),
        ("euro_yen", "2554"),
        ("pound_yen", "2556"),
        ("nzd_usd", "2557"),
        ("usd_swiss_franc", "2558"),
        ("usd_mexican_peso", "2559"),
        ("usd_singapore_dollar", "2561"),
        ("dollar_yuan", "2575"),
        ("bitcoin_usd_live", "2602"),
    ],
    
    "06_interest_rates": [
        ("libor_rates", "1433"),
        ("ted_spread", "1447"),
        ("federal_funds_rate", "2015"),
        ("treasury_10_year", "2016"),
        ("treasury_1_year", "2492"),
        ("libor_1_year", "2515"),
        ("libor_1_month", "2518"),
        ("libor_6_month", "2519"),
        ("libor_3_month", "2520"),
        ("treasury_30_year", "2521"),
        ("treasury_5_year", "2522"),
        ("mortgage_30_year_fixed", "2604"),
    ],
    
    "07_economy": [
        ("housing_starts", "1314"),
        ("unemployment_rate", "1316"),
        ("initial_jobless_claims", "1365"),
        ("retail_sales", "1371"),
        ("auto_light_truck_sales", "1372"),
        ("u6_unemployment_rate", "1377"),
        ("debt_to_gdp_ratio", "1381"),
        ("national_debt_by_president", "2023"),
        ("national_debt_by_year", "2496"),
        ("inflation_rate_by_year", "2497"),
        ("unemployment_rate_by_race", "2508"),
        ("unemployment_rate_by_education", "2509"),
        ("unemployment_rate_college_grads", "2510"),
        ("unemployment_rate_men_vs_women", "2511"),
        ("durable_goods_orders", "2582"),
        ("industrial_production", "2583"),
        ("inflation_expectation_5y5y", "2584"),
        ("capacity_utilization_rate", "2585"),
        ("black_unemployment_rate", "2621"),
        ("continued_jobless_claims", "2629"),
        ("jobless_claims_4_week_avg", "2630"),
        ("coronavirus_jobs_lost", "2632"),
    ],
}

def convert_timestamp_to_date(timestamp):
    """Convert Unix timestamp (milliseconds) to YYYY-MM-DD date string"""
    try:
        if isinstance(timestamp, (int, float)):
            # Handle milliseconds
            if abs(timestamp) > 1e11:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        return str(timestamp)
    except:
        return str(timestamp)

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

def fetch_iframe_data(page_id):
    """Fetch data from iframe endpoint for comparison charts"""
    # First, get the page to find the URL slug
    main_url = f"https://www.macrotrends.net/{page_id}/"
    try:
        resp = requests.get(main_url, headers=HEADERS, timeout=30, allow_redirects=True)
        final_url = resp.url
        url_slug = final_url.split('/')[-1] if '/' in final_url else ""
        
        if not url_slug:
            return []
        
        iframe_url = f"https://www.macrotrends.net/assets/php/chart_iframe_comp.php?id={page_id}&url={url_slug}"
        resp = requests.get(iframe_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
        
        # Extract originalData arrays
        pattern = r'var originalData\s*=\s*(\[.*?\]);'
        matches = re.findall(pattern, html, re.DOTALL)
        
        all_data = []
        for match in matches:
            try:
                data = json.loads(match)
                if data and len(data) > 0:
                    all_data.append(data)
            except:
                continue
        
        return all_data
    except Exception as e:
        return []

def process_and_save_data(data, filepath):
    """Process data (convert timestamps) and save to CSV"""
    if not data:
        return False
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        if isinstance(data[0], dict):
            # Dict format: {"date": "...", "close": "..."}
            headers = list(data[0].keys())
            writer.writerow(headers)
            for row in data:
                processed_row = []
                for h in headers:
                    val = row.get(h, '')
                    if h == 'date' and isinstance(val, str) and val:
                        processed_row.append(val)
                    else:
                        processed_row.append(val)
                writer.writerow(processed_row)
        elif isinstance(data[0], list):
            # List format: [timestamp, value] or [timestamp, value1, value2]
            if len(data[0]) == 2:
                writer.writerow(['date', 'value'])
            elif len(data[0]) == 3:
                writer.writerow(['date', 'value1', 'value2'])
            else:
                writer.writerow([f'col{i}' for i in range(len(data[0]))])
            
            for row in data:
                processed_row = list(row)
                # Convert first column if it looks like a timestamp
                if processed_row and isinstance(processed_row[0], (int, float)):
                    processed_row[0] = convert_timestamp_to_date(processed_row[0])
                writer.writerow(processed_row)
    
    return True

def scrape_chart(chart_name, page_id, section_folder):
    """Scrape all available data for a single chart"""
    chart_folder = os.path.join(section_folder, chart_name)
    os.makedirs(chart_folder, exist_ok=True)
    
    files_created = 0
    
    # Try API endpoints for all 4 graph types
    for graph_name, freq in GRAPH_TYPES:
        data = fetch_api_data(page_id, freq)
        if data:
            filepath = os.path.join(chart_folder, f"{graph_name}.csv")
            if process_and_save_data(data, filepath):
                print(f"      ✓ {graph_name}: {len(data)} records")
                files_created += 1
        time.sleep(0.5)
    
    # If no API data, try iframe (for comparison charts)
    if files_created == 0:
        iframe_data = fetch_iframe_data(page_id)
        if iframe_data:
            for i, data in enumerate(iframe_data):
                series_name = f"series_{i+1:02d}" if i > 0 else "main_data"
                filepath = os.path.join(chart_folder, f"{series_name}.csv")
                if process_and_save_data(data, filepath):
                    print(f"      ✓ {series_name}: {len(data)} records")
                    files_created += 1
    
    if files_created == 0:
        print(f"      ✗ No data available")
        # Remove empty folder
        try:
            os.rmdir(chart_folder)
        except:
            pass
    
    return files_created

def main():
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "macrotrends_data")
    os.makedirs(base_dir, exist_ok=True)
    
    print("=" * 70)
    print("MacroTrends Complete Data Scraper")
    print("=" * 70)
    print(f"Output: {base_dir}")
    
    total_charts = sum(len(charts) for charts in SECTIONS.values())
    print(f"Total sections: {len(SECTIONS)}")
    print(f"Total charts: {total_charts}")
    print("=" * 70)
    
    total_files = 0
    
    for section_name, charts in SECTIONS.items():
        print(f"\n{'='*70}")
        print(f"SECTION: {section_name.upper()}")
        print(f"{'='*70}")
        
        section_folder = os.path.join(base_dir, section_name)
        os.makedirs(section_folder, exist_ok=True)
        
        for chart_name, page_id in charts:
            print(f"\n  📊 {chart_name} (id: {page_id})")
            files = scrape_chart(chart_name, page_id, section_folder)
            total_files += files
            time.sleep(1)  # Rate limiting
    
    print("\n" + "=" * 70)
    print("SCRAPING COMPLETE!")
    print("=" * 70)
    print(f"Total files created: {total_files}")
    print(f"Output location: {base_dir}")
    print("=" * 70)

if __name__ == "__main__":
    main()
