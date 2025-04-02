from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import pytz
import time
import os
import logging
import json
from pathlib import Path
from browser_manager import create_browser_manager

# Configuration
SCREENER_URLS = {
    "price_crossover_200": "https://chartink.com/screener/price-corssover-200",
    "one_hour_rsi": "https://chartink.com/screener/1-hour-rsi-2119",
    "five_min_rsi": "https://chartink.com/screener/5-minutes-rsi-100"
}

MARKET_BULLISH_URL = "https://chartink.com/screener/for-a-bullish-trend"
MARKET_BEARISH_URL = "https://chartink.com/screener/for-a-bearish-trend"
RESULTS_FILE = "combined_scan_results.json"
MARKET_TREND_FILE = "market_trend.json"
SCAN_INTERVAL = 60  # Seconds
LOG_DIR = "logs"

# Create logs directory if it doesn't exist
Path(LOG_DIR).mkdir(exist_ok=True)

def setup_logging():
    """Configure logging with daily rotation"""
    log_file = os.path.join(LOG_DIR, f'combined_scanner_{datetime.now().strftime("%Y%m%d")}.log')
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    # Also log to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(console_handler)

def get_ist_time():
    """Get current time in IST"""
    return datetime.now(pytz.timezone('Asia/Kolkata'))

def is_market_hours():
    """Check if current time is within market hours"""
    now = get_ist_time()
    
    # Check if it's a weekday
    if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        logging.info("Market closed - Weekend")
        return False
    
    # Check if within trading hours (9:15 AM to 3:30 PM IST)
    market_start = now.replace(hour=0, minute=0, second=0)
    market_end = now.replace(hour=23, minute=59, second=0)
    
    is_open = market_start <= now <= market_end
    if not is_open:
        logging.info("Market closed - Outside trading hours")
    return is_open

def initialize_results_file():
    """Initialize results file with empty data"""
    try:
        if not os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE, 'w') as file:
                json.dump({
                    "last_update": "",
                    "market_bias": "unknown",
                    "screeners": {
                        "price_crossover_200": [],
                        "one_hour_rsi": [],
                        "five_min_rsi": []
                    },
                    "combined_stocks": []
                }, file, indent=2)
        logging.info(f"Results file initialized: {RESULTS_FILE}")
    except Exception as e:
        logging.error(f"Error initializing results file: {e}")
        raise

def save_scan_results(data):
    """Save scan results atomically with JSON format"""
    try:
        temp_file = f"{RESULTS_FILE}.tmp"
        
        with open(temp_file, 'w') as file:
            json.dump(data, file, indent=2)
        
        os.replace(temp_file, RESULTS_FILE)  # Atomic file write
        logging.info(f"Successfully saved scan results to file")
    except Exception as e:
        logging.error(f"Error saving to results file: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise

def load_current_results():
    """Load current scan results from file"""
    try:
        if os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE, 'r') as file:
                return json.load(file)
        else:
            return {
                "last_update": "",
                "market_bias": "unknown",
                "screeners": {
                    "price_crossover_200": [],
                    "one_hour_rsi": [],
                    "five_min_rsi": []
                },
                "combined_stocks": []
            }
    except Exception as e:
        logging.error(f"Error loading results file: {e}")
        return {
            "last_update": "",
            "market_bias": "unknown",
            "screeners": {
                "price_crossover_200": [],
                "one_hour_rsi": [],
                "five_min_rsi": []
            },
            "combined_stocks": []
        }

def load_page(driver, url):
    """Load page and wait for it to be ready"""
    try:
        driver.get(url)
        logging.info(f"Accessing {url}...")
        
        # Wait for page to fully load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(text(), 'Run Scan')]"))
        )
        logging.info("Page loaded successfully")
        return True
    except Exception as e:
        logging.error(f"Page load failed: {e}")
        return False

def run_scan(driver):
    """Click the Run Scan button"""
    try:
        # Using more specific and reliable CSS selector based on the screen information
        run_scan_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn.btn-success.run_scan_button"))
        )
        driver.execute_script("arguments[0].click();", run_scan_button)
        logging.info("Clicked Run Scan button...")
        
        # Wait for loading indicator to disappear
        WebDriverWait(driver, 60).until(
            EC.invisibility_of_element_located((By.XPATH, "//div[contains(text(), 'Running Screener...')]"))
        )
        logging.info("Scan completed successfully")
        return True
    except Exception as e:
        logging.error(f"Scan failed: {e}")
        return False

def validate_stock_data(symbol, price, change=None, volume=None):
    """Validate stock data before saving"""
    if not symbol or len(symbol) < 2:
        raise ValueError(f"Invalid symbol: {symbol}")
    
    try:
        if price:
            price = float(price.replace(',', ''))
        if change:
            change = float(change.replace('%', '').replace(',', ''))
        if volume:
            volume = int(volume.replace(',', ''))
    except (ValueError, AttributeError) as e:
        raise ValueError(f"Invalid value format: {e}")
        
    return True

def determine_stock_trend(change):
    """Determine if a stock is bullish or bearish based on price change"""
    try:
        change_val = float(change) if isinstance(change, str) else change
        return "bullish" if change_val > 0 else "bearish"
    except (ValueError, TypeError):
        return "unknown"

def extract_stock_data(driver, screener_type):
    """Extract stock data from the screener results"""
    try:
        table = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "DataTables_Table_0"))
        )
        
        stocks = []
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        
        for row in rows:
            try:
                symbol = row.find_element(By.CSS_SELECTOR, "td:nth-child(3) > a").text.strip()
                
                # Extract % Change (from column labeled "% Chg" in the screenshot)
                change_str = row.find_element(By.CSS_SELECTOR, "td:nth-child(5)").text.strip()
                
                # Extract price
                price_str = row.find_element(By.CSS_SELECTOR, "td:nth-child(6)").text.strip()
                
                # Extract volume
                volume_str = row.find_element(By.CSS_SELECTOR, "td:nth-child(7)").text.strip()
                
                if validate_stock_data(symbol, price_str, change_str, volume_str):
                    price = float(price_str.replace(',', ''))
                    change = float(change_str.replace('%', '').replace(',', ''))
                    volume = int(volume_str.replace(',', ''))
                    
                    # Determine if the stock is bullish or bearish based on change
                    stock_trend = determine_stock_trend(change)
                    
                    stocks.append({
                        'symbol': symbol,
                        'price': price,
                        'change': change,
                        'volume': volume,
                        'stock_trend': stock_trend,
                        'screener_type': screener_type
                    })
                    logging.info(f"Extracted from {screener_type}: {symbol} at ₹{price} (Change: {change}%, Volume: {volume}, Trend: {stock_trend})")
                
            except Exception as e:
                logging.error(f"Error processing row: {e}")
                continue
                
        return stocks
    except Exception as e:
        logging.error(f"Data extraction failed: {e}")
        return []

def filter_stocks_by_market_trend(stocks, market_bias):
    """Filter stocks based on market trend alignment"""
    if not stocks:
        return []
    
    filtered_stocks = []
    
    for stock in stocks:
        # Add market bias to each stock
        stock['market_bias'] = market_bias
        
        # When market is bullish, only include bullish stocks
        if market_bias == "bullish" and stock['stock_trend'] == "bullish":
            filtered_stocks.append(stock)
            logging.info(f"ALERT: Bullish stock {stock['symbol']} aligns with bullish market")
        
        # When market is bearish, only include bearish stocks
        elif market_bias == "bearish" and stock['stock_trend'] == "bearish":
            filtered_stocks.append(stock)
            logging.info(f"ALERT: Bearish stock {stock['symbol']} aligns with bearish market")
        
        # When market is neutral, include all stocks
        elif market_bias == "neutral":
            filtered_stocks.append(stock)
            logging.info(f"ALERT: {stock['stock_trend'].capitalize()} stock {stock['symbol']} in neutral market")
    
    filtered_count = len(filtered_stocks)
    total_count = len(stocks)
    logging.info(f"Filtered {filtered_count}/{total_count} stocks based on {market_bias} market trend")
    
    return filtered_stocks

def check_market_trend(driver, browser_mgr):
    """Check market trend using indices and return market bias"""
    market_bias = "unknown"
    bullish_indices = []
    bearish_indices = []
    
    # Check bullish trend
    if load_page(driver, MARKET_BULLISH_URL):
        if run_scan(driver):
            # Extract bullish indices
            try:
                table = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable"))
                )
                rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                for row in rows:
                    try:
                        symbol = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)").text.strip()
                        bullish_indices.append(symbol)
                    except:
                        continue
                logging.info(f"Bullish indices: {', '.join(bullish_indices)}")
            except Exception as e:
                logging.error(f"Error extracting bullish indices: {e}")
    
    # Check bearish trend
    if load_page(driver, MARKET_BEARISH_URL):
        if run_scan(driver):
            # Extract bearish indices
            try:
                table = WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "table.dataTable"))
                )
                rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                for row in rows:
                    try:
                        symbol = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)").text.strip()
                        bearish_indices.append(symbol)
                    except:
                        continue
                logging.info(f"Bearish indices: {', '.join(bearish_indices)}")
            except Exception as e:
                logging.error(f"Error extracting bearish indices: {e}")
    
    # Determine market bias
    key_indices = ["NIFTY", "BANKNIFTY", "NIFTYFINSERVICE"]
    bullish_count = sum(1 for idx in key_indices if idx in bullish_indices)
    bearish_count = sum(1 for idx in key_indices if idx in bearish_indices)
    
    if bullish_count > bearish_count:
        market_bias = "bullish"
    elif bearish_count > bullish_count:
        market_bias = "bearish"
    else:
        market_bias = "neutral"
    
    logging.info(f"Market bias determined as: {market_bias.upper()}")
    
    # Save market trend data to file
    market_data = {
        "timestamp": get_ist_time().strftime("%Y-%m-%d %H:%M:%S"),
        "market_bias": market_bias,
        "bullish_indices": bullish_indices,
        "bearish_indices": bearish_indices
    }
    
    with open(MARKET_TREND_FILE, 'w') as file:
        json.dump(market_data, file, indent=2)
    
    return market_bias

def find_stocks_in_multiple_screeners(results_data):
    """Find stocks that appear in multiple screeners"""
    # Extract symbols from each screener
    price_crossover_symbols = {stock['symbol'] for stock in results_data['screeners']['price_crossover_200']}
    one_hour_rsi_symbols = {stock['symbol'] for stock in results_data['screeners']['one_hour_rsi']}
    five_min_rsi_symbols = {stock['symbol'] for stock in results_data['screeners']['five_min_rsi']}
    
    # Find symbols present in at least 2 screeners
    combined_symbols = set()
    
    # Symbols in all three screeners
    symbols_in_all = price_crossover_symbols & one_hour_rsi_symbols & five_min_rsi_symbols
    if symbols_in_all:
        logging.info(f"Symbols in all three screeners: {', '.join(symbols_in_all)}")
        combined_symbols.update(symbols_in_all)
    
    # Symbols in price_crossover and one_hour_rsi
    symbols_in_pc_and_1h = price_crossover_symbols & one_hour_rsi_symbols - five_min_rsi_symbols
    if symbols_in_pc_and_1h:
        logging.info(f"Symbols in price_crossover and one_hour_rsi: {', '.join(symbols_in_pc_and_1h)}")
        combined_symbols.update(symbols_in_pc_and_1h)
    
    # Symbols in price_crossover and five_min_rsi
    symbols_in_pc_and_5m = price_crossover_symbols & five_min_rsi_symbols - one_hour_rsi_symbols
    if symbols_in_pc_and_5m:
        logging.info(f"Symbols in price_crossover and five_min_rsi: {', '.join(symbols_in_pc_and_5m)}")
        combined_symbols.update(symbols_in_pc_and_5m)
    
    # Symbols in one_hour_rsi and five_min_rsi
    symbols_in_1h_and_5m = one_hour_rsi_symbols & five_min_rsi_symbols - price_crossover_symbols
    if symbols_in_1h_and_5m:
        logging.info(f"Symbols in one_hour_rsi and five_min_rsi: {', '.join(symbols_in_1h_and_5m)}")
        combined_symbols.update(symbols_in_1h_and_5m)
    
    # Create combined stock list with full details
    combined_stocks = []
    
    # Dictionary to hold all stocks by symbol for quick lookup
    all_stocks_by_symbol = {}
    
    # Populate the dictionary with all stocks
    for screener_type, stocks_list in results_data['screeners'].items():
        for stock in stocks_list:
            if stock['symbol'] not in all_stocks_by_symbol:
                all_stocks_by_symbol[stock['symbol']] = []
            all_stocks_by_symbol[stock['symbol']].append(stock)
    
    # For each symbol in the combined list, create a consolidated entry
    for symbol in combined_symbols:
        stock_instances = all_stocks_by_symbol.get(symbol, [])
        if not stock_instances:
            continue
            
        # Use the most recent instance for base data
        base_stock = stock_instances[0]
        
        # List which screeners this stock appears in
        screeners_found_in = [s['screener_type'] for s in stock_instances]
        
        combined_stock = {
            'symbol': symbol,
            'price': base_stock['price'],
            'change': base_stock['change'],
            'volume': base_stock['volume'],
            'stock_trend': base_stock['stock_trend'],
            'market_bias': base_stock['market_bias'],
            'screeners_found_in': screeners_found_in,
            'match_count': len(screeners_found_in)
        }
        
        combined_stocks.append(combined_stock)
        logging.info(f"Combined stock: {symbol} found in {len(screeners_found_in)} screeners")
    
    return combined_stocks

def run_screener(driver, screener_type, url, market_bias, results_data):
    """Run specific screener scan and process results"""
    logging.info(f"Running {screener_type} screener...")
    
    if load_page(driver, url):
        if run_scan(driver):
            time.sleep(2)  # Short wait for table to stabilize
            
            # Extract all stocks from this screener
            all_stock_data = extract_stock_data(driver, screener_type)
            
            if all_stock_data:
                # Filter stocks based on market trend alignment
                filtered_stock_data = filter_stocks_by_market_trend(all_stock_data, market_bias)
                
                # Store in results
                results_data['screeners'][screener_type] = filtered_stock_data
                
                logging.info(f"{screener_type} scan completed. Found {len(filtered_stock_data)} stocks aligned with {market_bias} market bias")
            else:
                logging.warning(f"No stocks found in {screener_type} scan")
                results_data['screeners'][screener_type] = []
        else:
            logging.warning(f"{screener_type} scan failed. Will try again in the next cycle.")
    
    return results_data

def main():
    setup_logging()
    logging.info("Starting Combined ChartInk Scanner...")
    
    initialize_results_file()
    browser_mgr = create_browser_manager()
    driver = None
    
    try:
        driver = browser_mgr.initialize_browser('combined_scanner', headless=True)
        driver.set_page_load_timeout(30)
        
        # Main scan loop
        while True:
            if not is_market_hours():
                time.sleep(60)  # Check every minute during off-market hours
                continue
            
            logging.info(f"Starting market trend check at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}...")
            
            # First check market trend
            market_bias = check_market_trend(driver, browser_mgr)
            
            # Load current results
            results_data = load_current_results()
            results_data['market_bias'] = market_bias
            results_data['last_update'] = get_ist_time().strftime("%Y-%m-%d %H:%M:%S")
            
            # Run all three screeners
            for screener_type, url in SCREENER_URLS.items():
                results_data = run_screener(driver, screener_type, url, market_bias, results_data)
            
            # Find stocks that appear in multiple screeners
            combined_stocks = find_stocks_in_multiple_screeners(results_data)
            results_data['combined_stocks'] = combined_stocks
            
            # Save the combined results
            save_scan_results(results_data)
            
            logging.info(f"Complete scan cycle finished. Found {len(combined_stocks)} stocks in multiple screeners.")
            logging.info(f"Waiting {SCAN_INTERVAL} seconds for next scan...")
            time.sleep(SCAN_INTERVAL)
    
    except KeyboardInterrupt:
        logging.info("Scanner stopped by user")
    except Exception as e:
        logging.critical(f"Critical error: {e}")
    finally:
        if driver:
            browser_mgr.kill_browser('combined_scanner')
            logging.info("Browser closed")

if __name__ == "__main__":
    main()
