#!/usr/bin/env python3
"""
MacroTrends Complete Data Scraper v2
Properly handles both:
- Price charts (via API) - have 4 graph types
- Comparison charts (via iframe) - need URL slugs
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

GRAPH_TYPES = [
    ("01_historical", "INDEXMONTHLY"),
    ("02_inflation_adjusted", "INDEXMONTHLYINFLATION"),
    ("03_annual_change", "INDEXANNUALCHANGE"),
    ("04_annual_average", "INDEXANNUALAVG"),
]

# ============================================================================
# PRICE CHARTS - Use API endpoint (have 4 graph types)
# Format: (name, page_id)
# ============================================================================
PRICE_CHARTS = {
    "01_market_indexes": [
        ("dow_jones_100_year", "1319"),
        ("nasdaq_45_year", "1320"),
        ("dow_jones_10_year_daily", "1358"),
        ("sp500_90_year", "2324"),
        ("sp500_10_year_daily", "2488"),
        ("nasdaq_10_year_daily", "2489"),
        ("sp500_ytd_performance", "2490"),
        ("dow_jones_ytd_performance", "2505"),
        ("sp500_annual_returns", "2526"),
        ("nasdaq_ytd_performance", "2527"),
        ("shanghai_composite_china", "2592"),
        ("nikkei_225_japan", "2593"),
        ("hang_seng_hong_kong", "2594"),
        ("dax_30_germany", "2595"),
        ("cac_40_france", "2596"),
        ("bovespa_brazil", "2597"),
        ("vix_volatility_index", "2603"),
        ("dow_jones_by_year", "2622"),
        ("nasdaq_by_year", "2623"),
    ],
    "02_precious_metals": [
        ("gold_prices_100_year", "1333"),
        ("silver_prices_100_year", "1470"),
        ("platinum_prices", "2540"),
        ("gold_price_10_years", "2627"),
    ],
    "03_energy": [
        ("crude_oil_prices_70_year", "1369"),
        ("natural_gas_prices", "2478"),
        ("heating_oil_prices", "2479"),
        ("brent_crude_oil_10_year", "2480"),
        ("wti_crude_oil_10_year", "2516"),
        ("us_crude_oil_exports", "2563"),
        ("saudi_arabia_crude_production", "2564"),
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
    ],
    "06_interest_rates": [
        ("ted_spread", "1447"),
        ("federal_funds_rate", "2015"),
        ("treasury_10_year", "2016"),
        ("treasury_1_year", "2492"),
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
        ("national_debt_by_year", "2496"),
        ("inflation_rate_by_year", "2497"),
        ("unemployment_rate_college_grads", "2510"),
        ("durable_goods_orders", "2582"),
        ("industrial_production", "2583"),
        ("inflation_expectation_5y5y", "2584"),
        ("capacity_utilization_rate", "2585"),
        ("black_unemployment_rate", "2621"),
        ("continued_jobless_claims", "2629"),
        ("jobless_claims_4_week_avg", "2630"),
    ],
}

# ============================================================================
# COMPARISON CHARTS - Use iframe endpoint (need URL slugs)
# Format: (name, page_id, url_slug)
# ============================================================================
COMPARISON_CHARTS = {
    "01_market_indexes": [
        ("stock_market_secular_cycles", "1296", "stock-market-cycles-historical-chart"),
        ("sp500_earnings_history", "1324", "s-p-500-earnings-history"),
        ("stock_market_by_president", "2481", "stock-market-performance-by-president"),
        ("sp500_by_president", "2482", "sp500-performance-by-president"),
        ("dow_jones_1929_bear_market", "2484", "dow-jones-crash-1929-bear-market"),
        ("dow_vs_nasdaq", "2528", "dow-jones-vs-NASDAQ-chart"),
        ("dow_to_gdp_ratio", "2574", "dow-to-gdp-ratio-chart"),
        ("sp500_pe_ratio", "2577", "sp-500-pe-ratio-price-to-earnings-chart"),
        ("nasdaq_to_dow_ratio", "2600", "nasdaq-to-dow-ratio-chart"),
        ("sp500_vs_durable_goods", "2601", "sp-500-vs-durable-goods-chart"),
        ("stock_market_by_president_election", "2613", "stock-market-performance-by-president-from-election-date"),
        ("sp500_by_president_election", "2614", "sp500-performance-by-president-from-election-date"),
        ("trump_stock_market", "2616", "president-trump-stock-market-performance"),
    ],
    "02_precious_metals": [
        ("gold_vs_oil_prices", "1334", "gold-prices-vs-oil-prices-historical-correlation"),
        ("gold_vs_us_dollar", "1335", "dollar-vs-gold-comparison-last-ten-years"),
        ("dow_to_gold_ratio", "1378", "dow-to-gold-ratio-100-year-historical-chart"),
        ("gold_to_oil_ratio", "1380", "gold-to-oil-ratio-historical-chart"),
        ("sp500_to_gold_ratio", "1437", "sp500-to-gold-ratio-chart"),
        ("xau_to_gold_ratio", "1439", "xau-to-gold-ratio"),
        ("hui_to_gold_ratio", "1440", "hui-to-gold-ratio"),
        ("gold_to_silver_ratio", "1441", "gold-to-silver-ratio"),
        ("fed_balance_sheet_vs_gold", "1486", "fed-balance-sheet-vs-gold-price"),
        ("gold_to_monetary_base_ratio", "2485", "gold-to-monetary-base-ratio"),
        ("gold_vs_silver_prices", "2517", "gold-prices-vs-silver-prices-historical-chart"),
        ("platinum_vs_gold_prices", "2541", "platinum-prices-vs-gold-prices"),
        ("palladium_prices", "2542", "palladium-prices-historical-chart-data"),
        ("gold_prices_live", "2586", "gold-prices-today-live-chart"),
        ("silver_prices_live", "2589", "silver-prices-today-live-chart"),
        ("gold_vs_stock_market", "2608", "gold-price-vs-stock-market-100-year-chart"),
        ("dow_to_silver_ratio", "2610", "dow-to-silver-ratio-100-year-historical-chart"),
        ("silver_to_oil_ratio", "2612", "silver-to-oil-ratio-historical-chart"),
    ],
    "03_energy": [
        ("crude_oil_vs_sp500", "1453", "crude-oil-vs-the-s-p-500"),
        ("oil_vs_natural_gas", "2500", "crude-oil-vs-natural-gas-chart"),
        ("oil_vs_gasoline_prices", "2501", "crude-oil-vs-gasoline-prices-chart"),
        ("oil_vs_propane_prices", "2502", "crude-oil-vs-propane-prices-chart"),
        ("us_crude_oil_production", "2562", "us-crude-oil-production-historical-chart"),
        ("us_crude_oil_reserves", "2565", "us-crude-oil-reserves-historical-chart"),
        ("crude_oil_prices_live", "2566", "crude-oil-prices-today-live-chart"),
    ],
    "04_commodities": [
        ("copper_prices_live", "2590", "copper-prices-today-live-chart"),
    ],
    "05_exchange_rates": [
        ("bitcoin_usd_live", "2602", "bitcoin-usd-live-price-chart"),
    ],
    "06_interest_rates": [
        ("libor_rates", "1433", "historical-libor-rates-chart"),
        ("libor_1_year", "2515", "1-year-libor-rate-historical-chart"),
        ("libor_1_month", "2518", "1-month-libor-rate-historical-chart"),
        ("libor_6_month", "2519", "6-month-libor-rate-historical-chart"),
        ("libor_3_month", "2520", "3-month-libor-rate-historical-chart"),
    ],
    "07_economy": [
        ("national_debt_by_president", "2023", "national-debt-by-president"),
        ("unemployment_rate_by_race", "2508", "unemployment-rate-by-race"),
        ("unemployment_rate_by_education", "2509", "unemployment-rate-by-education"),
        ("unemployment_rate_men_vs_women", "2511", "unemployment-rate-men-women"),
        ("coronavirus_jobs_lost", "2632", "coronavirus-jobs-lost-vs-previous-recessions"),
    ],
}

def convert_timestamp_to_date(timestamp):
    """Convert Unix timestamp (milliseconds) to YYYY-MM-DD"""
    try:
        if isinstance(timestamp, (int, float)):
            if abs(timestamp) > 1e11:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
        return str(timestamp)
    except:
        return str(timestamp)

def fetch_api_data(page_id, frequency):
    """Fetch data from API for price charts"""
    url = f"https://www.macrotrends.net/economic-data/{page_id}/{frequency}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get('data', [])
    except:
        return []

def fetch_iframe_data(page_id, url_slug):
    """Fetch data from iframe for comparison charts"""
    url = f"https://www.macrotrends.net/assets/php/chart_iframe_comp.php?id={page_id}&url={url_slug}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        html = resp.text
        
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
    except:
        return []

def save_data(data, filepath):
    """Save data to CSV with proper date formatting"""
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
            elif len(data[0]) == 3:
                writer.writerow(['date', 'value1', 'value2'])
            else:
                writer.writerow([f'col{i}' for i in range(len(data[0]))])
            
            for row in data:
                processed = list(row)
                if processed and isinstance(processed[0], (int, float)):
                    processed[0] = convert_timestamp_to_date(processed[0])
                writer.writerow(processed)
    return True

def scrape_price_chart(name, page_id, section_folder):
    """Scrape price chart via API (4 graph types)"""
    chart_folder = os.path.join(section_folder, name)
    files_created = 0
    
    for graph_name, freq in GRAPH_TYPES:
        data = fetch_api_data(page_id, freq)
        if data:
            filepath = os.path.join(chart_folder, f"{graph_name}.csv")
            if save_data(data, filepath):
                print(f"      ✓ {graph_name}: {len(data)} records")
                files_created += 1
        time.sleep(0.5)
    
    if files_created == 0:
        print(f"      ✗ No data")
    return files_created

def scrape_comparison_chart(name, page_id, url_slug, section_folder):
    """Scrape comparison chart via iframe"""
    chart_folder = os.path.join(section_folder, name)
    
    data_arrays = fetch_iframe_data(page_id, url_slug)
    if not data_arrays:
        print(f"      ✗ No data")
        return 0
    
    files_created = 0
    for i, data in enumerate(data_arrays):
        series_name = f"series_{i+1:02d}" if i > 0 else "main_data"
        filepath = os.path.join(chart_folder, f"{series_name}.csv")
        if save_data(data, filepath):
            print(f"      ✓ {series_name}: {len(data)} records")
            files_created += 1
    
    return files_created

def main():
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "macrotrends_data_v2")
    os.makedirs(base_dir, exist_ok=True)
    
    print("=" * 70)
    print("MacroTrends Complete Scraper v2")
    print("=" * 70)
    print(f"Output: {base_dir}")
    
    total_files = 0
    
    # Get all section names
    all_sections = set(PRICE_CHARTS.keys()) | set(COMPARISON_CHARTS.keys())
    
    for section in sorted(all_sections):
        print(f"\n{'='*70}")
        print(f"SECTION: {section.upper()}")
        print(f"{'='*70}")
        
        section_folder = os.path.join(base_dir, section)
        os.makedirs(section_folder, exist_ok=True)
        
        # Scrape price charts
        if section in PRICE_CHARTS:
            print("\n  [PRICE CHARTS - via API]")
            for name, page_id in PRICE_CHARTS[section]:
                print(f"\n    📊 {name}")
                total_files += scrape_price_chart(name, page_id, section_folder)
                time.sleep(1)
        
        # Scrape comparison charts
        if section in COMPARISON_CHARTS:
            print("\n  [COMPARISON CHARTS - via iframe]")
            for name, page_id, url_slug in COMPARISON_CHARTS[section]:
                print(f"\n    📊 {name}")
                total_files += scrape_comparison_chart(name, page_id, url_slug, section_folder)
                time.sleep(1)
    
    print("\n" + "=" * 70)
    print("COMPLETE!")
    print(f"Total files: {total_files}")
    print(f"Output: {base_dir}")
    print("=" * 70)

if __name__ == "__main__":
    main()
