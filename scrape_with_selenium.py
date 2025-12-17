#!/usr/bin/env python3
"""
Selenium-based scraper for MacroTrends Precious Metals Data
Handles both simple price charts and comparison/ratio charts
"""

import os
import csv
import time
import re
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# All precious metals charts
CHARTS = [
    ("Gold_Prices_100_Year", "https://www.macrotrends.net/1333/historical-gold-prices-100-year-chart"),
    ("Gold_vs_Oil_Prices", "https://www.macrotrends.net/1334/gold-prices-vs-oil-prices-historical-correlation"),
    ("Gold_vs_US_Dollar", "https://www.macrotrends.net/1335/dollar-vs-gold-comparison-last-ten-years"),
    ("Dow_to_Gold_Ratio", "https://www.macrotrends.net/1378/dow-to-gold-ratio-100-year-historical-chart"),
    ("Gold_to_Oil_Ratio", "https://www.macrotrends.net/1380/gold-to-oil-ratio-historical-chart"),
    ("SP500_to_Gold_Ratio", "https://www.macrotrends.net/1437/sp500-to-gold-ratio-chart"),
    ("XAU_to_Gold_Ratio", "https://www.macrotrends.net/1439/xau-to-gold-ratio"),
    ("HUI_to_Gold_Ratio", "https://www.macrotrends.net/1440/hui-to-gold-ratio"),
    ("Gold_to_Silver_Ratio", "https://www.macrotrends.net/1441/gold-to-silver-ratio"),
    ("Silver_Prices_100_Year", "https://www.macrotrends.net/1470/historical-silver-prices-100-year-chart"),
    ("Fed_Balance_Sheet_vs_Gold", "https://www.macrotrends.net/1486/fed-balance-sheet-vs-gold-price"),
    ("Gold_to_Monetary_Base_Ratio", "https://www.macrotrends.net/2485/gold-to-monetary-base-ratio"),
    ("Gold_vs_Silver_Prices", "https://www.macrotrends.net/2517/gold-prices-vs-silver-prices-historical-chart"),
    ("Platinum_Prices", "https://www.macrotrends.net/2540/platinum-prices-historical-chart-data"),
    ("Platinum_vs_Gold_Prices", "https://www.macrotrends.net/2541/platinum-prices-vs-gold-prices"),
    ("Palladium_Prices", "https://www.macrotrends.net/2542/palladium-prices-historical-chart-data"),
    ("Gold_Prices_Live", "https://www.macrotrends.net/2586/gold-prices-today-live-chart"),
    ("Silver_Prices_Live", "https://www.macrotrends.net/2589/silver-prices-today-live-chart"),
    ("Gold_vs_Stock_Market", "https://www.macrotrends.net/2608/gold-price-vs-stock-market-100-year-chart"),
    ("Dow_to_Silver_Ratio", "https://www.macrotrends.net/2610/dow-to-silver-ratio-100-year-historical-chart"),
    ("Silver_to_Oil_Ratio", "https://www.macrotrends.net/2612/silver-to-oil-ratio-historical-chart"),
    ("Gold_Price_10_Years", "https://www.macrotrends.net/2627/gold-price-last-ten-years"),
]

def setup_driver():
    """Setup Chrome driver with headless options"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
    
    driver = webdriver.Chrome(options=options)
    return driver

def extract_chart_data_from_page(driver):
    """Extract chart data from the current page using JavaScript"""
    
    # Wait for Highcharts to load
    time.sleep(3)
    
    # Try to extract data from Highcharts
    try:
        # Get all Highcharts chart objects
        script = """
        var allData = [];
        if (typeof Highcharts !== 'undefined' && Highcharts.charts) {
            Highcharts.charts.forEach(function(chart) {
                if (chart && chart.series) {
                    chart.series.forEach(function(series) {
                        if (series.data && series.data.length > 0) {
                            var seriesData = {
                                name: series.name || 'Series',
                                data: series.data.map(function(point) {
                                    return {
                                        x: point.x,
                                        y: point.y,
                                        date: point.category || (point.x ? new Date(point.x).toISOString().split('T')[0] : null)
                                    };
                                })
                            };
                            allData.push(seriesData);
                        }
                    });
                }
            });
        }
        return JSON.stringify(allData);
        """
        result = driver.execute_script(script)
        return json.loads(result) if result else []
    except Exception as e:
        print(f"    Error extracting Highcharts data: {e}")
        return []

def extract_table_data(driver):
    """Extract data from HTML tables on the page"""
    try:
        tables = driver.find_elements(By.CSS_SELECTOR, 'table.historical_data_table, table[id*="datatable"]')
        all_data = []
        
        for table in tables:
            rows = table.find_elements(By.TAG_NAME, 'tr')
            for row in rows[1:]:  # Skip header
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) >= 2:
                    try:
                        date = cells[0].text.strip()
                        value = cells[1].text.strip().replace('$', '').replace(',', '')
                        if date and value:
                            all_data.append([date, float(value)])
                    except:
                        continue
        return all_data
    except Exception as e:
        print(f"    Error extracting table data: {e}")
        return []

def get_chart_tabs(driver):
    """Find and return available chart tabs/buttons"""
    tabs = []
    try:
        # Look for tab buttons
        tab_elements = driver.find_elements(By.CSS_SELECTOR, 
            'button[data-frequency], .chart-tab, [class*="tab"], select option')
        
        for elem in tab_elements:
            text = elem.text.strip()
            freq = elem.get_attribute('data-frequency') or elem.get_attribute('value')
            if text and freq:
                tabs.append((text, freq, elem))
    except:
        pass
    return tabs

def save_to_csv(data, filepath):
    """Save data to CSV file"""
    if not data:
        return False
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        if isinstance(data[0], dict):
            if 'date' in data[0]:
                writer.writerow(['date', 'value'])
                for row in data:
                    writer.writerow([row.get('date', ''), row.get('y', row.get('value', ''))])
            else:
                writer.writerow(data[0].keys())
                for row in data:
                    writer.writerow(row.values())
        elif isinstance(data[0], list):
            if len(data[0]) == 2:
                writer.writerow(['date', 'value'])
            else:
                writer.writerow([f'col{i}' for i in range(len(data[0]))])
            for row in data:
                writer.writerow(row)
    
    return True

def scrape_chart(driver, chart_name, url, base_output_dir):
    """Scrape all graph types from a single chart page"""
    print(f"\n{'='*50}")
    print(f"Chart: {chart_name}")
    print(f"URL: {url}")
    print(f"{'='*50}")
    
    chart_folder = os.path.join(base_output_dir, chart_name)
    os.makedirs(chart_folder, exist_ok=True)
    
    try:
        driver.get(url)
        time.sleep(5)  # Wait for page to load
        
        # Extract main chart data
        chart_data = extract_chart_data_from_page(driver)
        
        if chart_data:
            for i, series in enumerate(chart_data):
                series_name = series.get('name', f'Series_{i}').replace(' ', '_').replace('/', '_')
                data = series.get('data', [])
                if data:
                    filepath = os.path.join(chart_folder, f"{series_name}.csv")
                    if save_to_csv(data, filepath):
                        print(f"  ✓ {series_name}: {len(data)} records")
        
        # Also try to get table data
        table_data = extract_table_data(driver)
        if table_data:
            filepath = os.path.join(chart_folder, "table_data.csv")
            if save_to_csv(table_data, filepath):
                print(f"  ✓ table_data: {len(table_data)} records")
        
        # Try clicking different tabs/frequencies
        tabs = get_chart_tabs(driver)
        for tab_name, freq, elem in tabs:
            try:
                elem.click()
                time.sleep(2)
                
                tab_data = extract_chart_data_from_page(driver)
                if tab_data:
                    for series in tab_data:
                        series_name = f"{tab_name}_{series.get('name', 'data')}".replace(' ', '_')
                        data = series.get('data', [])
                        if data:
                            filepath = os.path.join(chart_folder, f"{series_name}.csv")
                            if save_to_csv(data, filepath):
                                print(f"  ✓ {series_name}: {len(data)} records")
            except:
                continue
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False

def main():
    base_output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "precious_metals_selenium")
    os.makedirs(base_output_dir, exist_ok=True)
    
    print("=" * 60)
    print("MacroTrends Precious Metals - Selenium Scraper")
    print("=" * 60)
    print(f"Output: {base_output_dir}")
    print(f"Charts: {len(CHARTS)}")
    
    driver = setup_driver()
    
    try:
        successful = 0
        for chart_name, url in CHARTS:
            if scrape_chart(driver, chart_name, url, base_output_dir):
                successful += 1
            time.sleep(3)
        
        print(f"\n{'='*60}")
        print(f"Complete! Processed {successful}/{len(CHARTS)} charts")
        print(f"{'='*60}")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
