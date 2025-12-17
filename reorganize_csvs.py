#!/usr/bin/env python3
"""
Reorganize and rename MacroTrends CSVs with proper naming convention:
{item}_{currency}_{frequency}_{timespan}.csv

Example: gold_usd_monthly_100years.csv
"""

import os
import csv
import shutil
from datetime import datetime
from collections import defaultdict

BASE_DIR = "/Users/s0m13i5/walmart/kafka-db2-projects/precious_metals_data/macrotrends_data_v2"
OUTPUT_DIR = "/Users/s0m13i5/walmart/kafka-db2-projects/precious_metals_data/macrotrends_organized"

# Mapping chart names to (item_name, unit, category)
# Units: usd, percent, ratio, index, barrels, units, exchange_rate
CHART_MAPPINGS = {
    # Market Indexes (measured in index points)
    "dow_jones_100_year": ("dow_jones", "index", "market"),
    "dow_jones_10_year_daily": ("dow_jones", "index", "market"),
    "dow_jones_by_year": ("dow_jones", "index", "market"),
    "dow_jones_ytd_performance": ("dow_jones", "index", "market"),
    "nasdaq_45_year": ("nasdaq", "index", "market"),
    "nasdaq_10_year_daily": ("nasdaq", "index", "market"),
    "nasdaq_by_year": ("nasdaq", "index", "market"),
    "nasdaq_ytd_performance": ("nasdaq", "index", "market"),
    "sp500_90_year": ("sp500", "index", "market"),
    "sp500_10_year_daily": ("sp500", "index", "market"),
    "sp500_annual_returns": ("sp500", "percent", "market"),
    "sp500_ytd_performance": ("sp500", "index", "market"),
    "shanghai_composite_china": ("shanghai_composite", "index", "market"),
    "nikkei_225_japan": ("nikkei_225", "index", "market"),
    "hang_seng_hong_kong": ("hang_seng", "index", "market"),
    "dax_30_germany": ("dax_30", "index", "market"),
    "cac_40_france": ("cac_40", "index", "market"),
    "bovespa_brazil": ("bovespa", "index", "market"),
    "vix_volatility_index": ("vix", "index", "market"),
    "stock_market_secular_cycles": ("stock_market_cycles", "index", "market"),
    "sp500_earnings_history": ("sp500_earnings", "usd", "market"),
    "stock_market_by_president": ("stock_market_president", "percent", "market"),
    "sp500_by_president": ("sp500_president", "percent", "market"),
    "dow_jones_1929_bear_market": ("dow_jones_1929_crash", "index", "market"),
    "dow_vs_nasdaq": ("dow_vs_nasdaq", "ratio", "market"),
    "dow_to_gdp_ratio": ("dow_gdp", "ratio", "market"),
    "sp500_pe_ratio": ("sp500_pe", "ratio", "market"),
    "nasdaq_to_dow_ratio": ("nasdaq_dow", "ratio", "market"),
    "sp500_vs_durable_goods": ("sp500_durable_goods", "ratio", "market"),
    "stock_market_by_president_election": ("stock_market_election", "percent", "market"),
    "sp500_by_president_election": ("sp500_election", "percent", "market"),
    "trump_stock_market": ("trump_stock_market", "index", "market"),
    
    # Precious Metals (prices in USD per ounce)
    "gold_prices_100_year": ("gold", "usd", "metals"),
    "gold_price_10_years": ("gold", "usd", "metals"),
    "silver_prices_100_year": ("silver", "usd", "metals"),
    "platinum_prices": ("platinum", "usd", "metals"),
    "palladium_prices": ("palladium", "usd", "metals"),
    "gold_vs_oil_prices": ("gold_vs_oil", "usd", "metals"),
    "gold_vs_us_dollar": ("gold_vs_dollar_index", "usd", "metals"),
    "dow_to_gold_ratio": ("dow_gold", "ratio", "metals"),
    "gold_to_oil_ratio": ("gold_oil", "ratio", "metals"),
    "sp500_to_gold_ratio": ("sp500_gold", "ratio", "metals"),
    "xau_to_gold_ratio": ("xau_gold", "ratio", "metals"),
    "hui_to_gold_ratio": ("hui_gold", "ratio", "metals"),
    "gold_to_silver_ratio": ("gold_silver", "ratio", "metals"),
    "gold_to_monetary_base_ratio": ("gold_monetary_base", "ratio", "metals"),
    "gold_vs_silver_prices": ("gold_vs_silver", "usd", "metals"),
    "platinum_vs_gold_prices": ("platinum_vs_gold", "usd", "metals"),
    "dow_to_silver_ratio": ("dow_silver", "ratio", "metals"),
    "silver_to_oil_ratio": ("silver_oil", "ratio", "metals"),
    
    # Energy (prices in USD, production in barrels)
    "crude_oil_prices_70_year": ("crude_oil", "usd", "energy"),
    "natural_gas_prices": ("natural_gas", "usd", "energy"),
    "heating_oil_prices": ("heating_oil", "usd", "energy"),
    "brent_crude_oil_10_year": ("brent_crude", "usd", "energy"),
    "wti_crude_oil_10_year": ("wti_crude", "usd", "energy"),
    "us_crude_oil_exports": ("us_oil_exports", "barrels", "energy"),
    "saudi_arabia_crude_production": ("saudi_oil_production", "barrels", "energy"),
    "crude_oil_vs_sp500": ("oil_vs_sp500", "usd", "energy"),
    "oil_vs_natural_gas": ("oil_vs_natural_gas", "usd", "energy"),
    "oil_vs_gasoline_prices": ("oil_vs_gasoline", "usd", "energy"),
    "oil_vs_propane_prices": ("oil_vs_propane", "usd", "energy"),
    "us_crude_oil_production": ("us_oil_production", "barrels", "energy"),
    "us_crude_oil_reserves": ("us_oil_reserves", "barrels", "energy"),
    
    # Commodities (prices in USD)
    "copper_prices": ("copper", "usd", "commodities"),
    "soybean_prices": ("soybean", "usd", "commodities"),
    "corn_prices": ("corn", "usd", "commodities"),
    "cotton_prices": ("cotton", "usd", "commodities"),
    "wheat_prices": ("wheat", "usd", "commodities"),
    "coffee_prices": ("coffee", "usd", "commodities"),
    "oats_prices": ("oats", "usd", "commodities"),
    "sugar_prices": ("sugar", "usd", "commodities"),
    "soybean_oil_prices": ("soybean_oil", "usd", "commodities"),
    "lumber_prices": ("lumber", "usd", "commodities"),
    
    # Exchange Rates (currency pairs)
    "us_dollar_index": ("dollar_index", "index", "forex"),
    "euro_dollar": ("eur_usd", "exchange", "forex"),
    "pound_dollar": ("gbp_usd", "exchange", "forex"),
    "dollar_yen": ("usd_jpy", "exchange", "forex"),
    "aud_usd": ("aud_usd", "exchange", "forex"),
    "euro_swiss_franc": ("eur_chf", "exchange", "forex"),
    "euro_pound": ("eur_gbp", "exchange", "forex"),
    "euro_yen": ("eur_jpy", "exchange", "forex"),
    "pound_yen": ("gbp_jpy", "exchange", "forex"),
    "nzd_usd": ("nzd_usd", "exchange", "forex"),
    "usd_swiss_franc": ("usd_chf", "exchange", "forex"),
    "usd_mexican_peso": ("usd_mxn", "exchange", "forex"),
    "usd_singapore_dollar": ("usd_sgd", "exchange", "forex"),
    "dollar_yuan": ("usd_cny", "exchange", "forex"),
    
    # Interest Rates (all in percent)
    "ted_spread": ("ted_spread", "percent", "rates"),
    "federal_funds_rate": ("fed_funds_rate", "percent", "rates"),
    "treasury_10_year": ("treasury_10y", "percent", "rates"),
    "treasury_1_year": ("treasury_1y", "percent", "rates"),
    "treasury_30_year": ("treasury_30y", "percent", "rates"),
    "treasury_5_year": ("treasury_5y", "percent", "rates"),
    "mortgage_30_year_fixed": ("mortgage_30y", "percent", "rates"),
    "libor_rates": ("libor", "percent", "rates"),
    "libor_1_year": ("libor_1y", "percent", "rates"),
    "libor_1_month": ("libor_1m", "percent", "rates"),
    "libor_6_month": ("libor_6m", "percent", "rates"),
    "libor_3_month": ("libor_3m", "percent", "rates"),
    
    # Economy (mixed units)
    "housing_starts": ("housing_starts", "units", "economy"),
    "unemployment_rate": ("unemployment", "percent", "economy"),
    "initial_jobless_claims": ("jobless_claims", "units", "economy"),
    "retail_sales": ("retail_sales", "usd", "economy"),
    "auto_light_truck_sales": ("auto_sales", "units", "economy"),
    "u6_unemployment_rate": ("u6_unemployment", "percent", "economy"),
    "debt_to_gdp_ratio": ("debt_gdp", "percent", "economy"),
    "national_debt_by_year": ("national_debt", "usd", "economy"),
    "inflation_rate_by_year": ("inflation", "percent", "economy"),
    "unemployment_rate_college_grads": ("unemployment_college", "percent", "economy"),
    "durable_goods_orders": ("durable_goods", "usd", "economy"),
    "industrial_production": ("industrial_production", "index", "economy"),
    "inflation_expectation_5y5y": ("inflation_expectation", "percent", "economy"),
    "capacity_utilization_rate": ("capacity_utilization", "percent", "economy"),
    "black_unemployment_rate": ("black_unemployment", "percent", "economy"),
    "continued_jobless_claims": ("continued_claims", "units", "economy"),
    "jobless_claims_4_week_avg": ("jobless_claims_avg", "units", "economy"),
    "national_debt_by_president": ("debt_president", "usd", "economy"),
    "unemployment_rate_by_race": ("unemployment_race", "percent", "economy"),
    "unemployment_rate_by_education": ("unemployment_education", "percent", "economy"),
    "unemployment_rate_men_vs_women": ("unemployment_gender", "percent", "economy"),
    "coronavirus_jobs_lost": ("covid_jobs", "units", "economy"),
}

# Graph type suffixes
GRAPH_TYPE_MAP = {
    "01_historical": "historical",
    "02_inflation_adjusted": "inflation_adj",
    "03_annual_change": "annual_change",
    "04_annual_average": "annual_avg",
    "main_data": "main",
    "series_02": "series2",
    "series_03": "series3",
}

def detect_frequency(dates):
    """Detect if data is daily, monthly, or yearly based on date intervals"""
    if len(dates) < 2:
        return "unknown"
    
    # Parse dates
    parsed = []
    for d in dates[:100]:  # Check first 100 dates
        try:
            if isinstance(d, str):
                parsed.append(datetime.strptime(d, "%Y-%m-%d"))
        except:
            continue
    
    if len(parsed) < 2:
        return "unknown"
    
    # Calculate average days between entries
    diffs = []
    for i in range(1, len(parsed)):
        diff = (parsed[i] - parsed[i-1]).days
        if diff > 0:
            diffs.append(diff)
    
    if not diffs:
        return "unknown"
    
    avg_diff = sum(diffs) / len(diffs)
    
    if avg_diff <= 7:
        return "daily"
    elif avg_diff <= 45:
        return "monthly"
    elif avg_diff <= 120:
        return "quarterly"
    else:
        return "yearly"

def detect_timespan(dates):
    """Calculate the total time span in years"""
    if len(dates) < 2:
        return "unknown"
    
    # Get first and last dates only
    first_date = None
    last_date = None
    
    for d in dates[:5]:  # Check first few
        try:
            if isinstance(d, str) and len(d) >= 10:
                first_date = datetime.strptime(d[:10], "%Y-%m-%d")
                break
        except:
            continue
    
    for d in reversed(dates[-5:]):  # Check last few
        try:
            if isinstance(d, str) and len(d) >= 10:
                last_date = datetime.strptime(d[:10], "%Y-%m-%d")
                break
        except:
            continue
    
    if not first_date or not last_date:
        return "unknown"
    
    years = abs((last_date - first_date).days) / 365.25
    
    # Round to sensible values
    if years <= 5:
        return "5y"
    elif years <= 12:
        return "10y"
    elif years <= 25:
        return "20y"
    elif years <= 40:
        return "30y"
    elif years <= 55:
        return "50y"
    elif years <= 75:
        return "70y"
    elif years <= 120:
        return "100y"
    elif years <= 150:
        return "120y"
    else:
        return "100y_plus"

def read_csv_dates(filepath):
    """Read dates from CSV file"""
    dates = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if row:
                    dates.append(row[0])
    except:
        pass
    return dates

def process_files():
    """Process all CSV files and rename them"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    results = defaultdict(list)
    
    for section in sorted(os.listdir(BASE_DIR)):
        section_path = os.path.join(BASE_DIR, section)
        if not os.path.isdir(section_path):
            continue
        
        output_section = os.path.join(OUTPUT_DIR, section)
        os.makedirs(output_section, exist_ok=True)
        
        print(f"\n{'='*60}")
        print(f"SECTION: {section}")
        print(f"{'='*60}")
        
        for chart_name in sorted(os.listdir(section_path)):
            chart_path = os.path.join(section_path, chart_name)
            if not os.path.isdir(chart_path):
                continue
            
            # Get mapping for this chart
            mapping = CHART_MAPPINGS.get(chart_name)
            if not mapping:
                print(f"  ⚠️  No mapping for: {chart_name}")
                item_name = chart_name
                unit = "unknown"
            else:
                item_name, unit, _ = mapping
            
            for csv_file in sorted(os.listdir(chart_path)):
                if not csv_file.endswith('.csv'):
                    continue
                
                csv_path = os.path.join(chart_path, csv_file)
                graph_type = csv_file.replace('.csv', '')
                
                # Read dates to determine frequency and timespan
                dates = read_csv_dates(csv_path)
                frequency = detect_frequency(dates)
                timespan = detect_timespan(dates)
                
                # Get graph type suffix
                type_suffix = GRAPH_TYPE_MAP.get(graph_type, graph_type)
                
                # Build new filename: {item}_{type}_{unit}_{frequency}_{timespan}.csv
                # Example: inflation_expectation_annual_avg_percent_yearly_20y.csv
                new_name = f"{item_name}_{type_suffix}_{unit}_{frequency}_{timespan}.csv"
                new_path = os.path.join(output_section, new_name)
                
                # Copy file
                shutil.copy2(csv_path, new_path)
                
                print(f"  ✓ {new_name}")
                results[section].append(new_name)
    
    return results

def main():
    print("=" * 60)
    print("Reorganizing MacroTrends CSV Files")
    print("=" * 60)
    print(f"Source: {BASE_DIR}")
    print(f"Output: {OUTPUT_DIR}")
    
    results = process_files()
    
    print("\n" + "=" * 60)
    print("COMPLETE!")
    print("=" * 60)
    
    total = 0
    for section, files in sorted(results.items()):
        print(f"{section}: {len(files)} files")
        total += len(files)
    
    print(f"\nTotal files: {total}")
    print(f"Output: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
