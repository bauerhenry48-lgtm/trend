from datetime import datetime
import time
import requests
import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt

from crps_calculation import calculate_crps_for_miner


from price_simulation import (
    simulate_crypto_price_paths,
)

from define import symbol_list

def fetch_ohlcv(close_time, symbol='BTC', interval='Min5', days_history=4, days_offset=0, limit=600):
    """Improved data fetcher with retry logic"""

    TOKEN_MAP = {
        "BTC": "Crypto.BTC/USD",
        "ETH": "Crypto.ETH/USD",
        "XAU": "Metal.XAU/USD",
        "SOL": "Crypto.SOL/USD",
    }

    end_time = int(close_time.timestamp()) - (days_offset * 86400)
    start_time = end_time - (days_history * 86400)

    result = {'t': [], 'c': []}

    try:
        response = requests.get(
            f"https://benchmarks.pyth.network/v1/shims/tradingview/history",
            params={'symbol': TOKEN_MAP[symbol], "resolution": 1, "from": start_time, "to": end_time}
        )
        data = response.json()

        timestamps = data["t"]
        close_prices = data["c"]

        start_time = timestamps[0]
        for t, c in zip(timestamps, close_prices):
            if t >= start_time and (t - start_time) % 300 == 0:
                result['t'].append(t)
                result['c'].append(float(c))
                
    except Exception as e:
        print(f"Error: {e}. Retrying...")

    df = pd.DataFrame(result)
    df = df[["t", "c"]].rename(columns={
        "t": "time",
        "c": "close",
    })
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df['time_utc'] = df['time'].dt.tz_localize('UTC')
    df['time_utc9'] = df['time'].dt.tz_localize('UTC')
    df['time_utc9'] = df['time_utc9'].dt.tz_convert('Asia/Tokyo')
    df.set_index('time', inplace=True)

    return df.dropna()

def calculate_crps(
    data, sigma, offset=0, length=289  # 1 day
):
    start_index = len(data['close']) - length - offset
    if start_index < 0 :
        return 0
    if start_index + length > len(data['close']) :
        return 0

    closePrice = data['close'].iloc[start_index]
    real_path = data['close'][start_index : start_index + length]
    close_time = data.index[start_index]
    price_paths, average_price = simulate_crypto_price_paths(closePrice, 300, 300 * (length - 1), 100, sigma, close_time)
    numeric_value, dict_list = calculate_crps_for_miner(price_paths, np.array(real_path), 300)
    return numeric_value

def calculate_sigma(symbol, offset=0):
    symbol_data = next((item for item in symbol_list if item['asset'] == symbol), None)

    best_sigma = 0
    best_crps = 1000000
    data = fetch_ohlcv(symbol=symbol, interval='Min5', days_history=2, days_offset=0, limit=600)
    # min_max_dev = get_value_from_file(symbol, 'min_max_dev')
    for s in range(0, symbol_data['search_count']) :
        sigma = round(symbol_data['origin_sigma'] + s * symbol_data['sigma_interval'], symbol_data['round_num'])
        crps = calculate_crps(data, sigma, offset)
        
        if crps > 0 and crps < best_crps :
            best_sigma = sigma
            best_crps = crps

    return best_sigma

def plot_real_path_basic(real_path, price_paths, data, start_index, j, average_price=None):
    """
    Modified version that works with both old and new calling patterns.
    """
    plt.figure(figsize=(14, 8))
    time_axis = data.index[start_index: start_index + len(real_path)]
    
    # Check if price_paths is a tuple (new) or array (old)
    if isinstance(price_paths, tuple):
        # New style: unpack tuple
        paths_array, sim_avg = price_paths
    else:
        # Old style: price_paths is just the array
        paths_array = price_paths
        # Calculate average from paths if not provided
        sim_avg = average_price if average_price is not None else np.mean(paths_array)
    
    # Calculate real path average
    real_avg = np.mean(real_path)
    
    # Plot simulated paths
    for i in range(len(paths_array)):
        plt.plot(time_axis, paths_array[i], alpha=0.5, linewidth=0.7)
    
    # Plot real path
    plt.plot(time_axis, real_path, linewidth=3, label="Real Path")
    
    # Plot average lines
    plt.axhline(y=sim_avg, color='red', linewidth=2.5, 
                linestyle='--', label=f"Simulated Avg: ${sim_avg:,.0f}")
    plt.axhline(y=real_avg, color='green', linewidth=2.5,
                linestyle='--', label=f"Real Path Avg: ${real_avg:,.0f}")
    
    plt.xlabel("Date & Time")
    plt.ylabel("Price")
    plt.title("Real Path vs Simulated Paths")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.savefig(f"real_vs_simulated_{j}.png", dpi=300, bbox_inches="tight")
    plt.close()

if __name__=="__main__":
    symbol = 'BTC'
    bulb = 0
    if bulb:
        best_sigma = calculate_sigma(symbol)
        print(best_sigma)
    else:    
        sigma = 0.0042
        currenttime = [

      datetime(2025, 12, 13, 0, 5, 0),
      datetime(2025, 12, 20, 0, 5, 0),
   
]

        for i in range(0, 2):
            current_time = currenttime[i]
            data = fetch_ohlcv(current_time, symbol=symbol, interval='Min5', days_history=2, days_offset=0)

            length = 289
            n = len(data["close"])
            start_index = n - length
            close_time = data.index[start_index]

            if isinstance(close_time, pd.Timestamp):
                close_time = close_time.to_pydatetime().replace(tzinfo=None)
            elif not isinstance(close_time, datetime):
                    print(f"Error: close_time must be datetime, got {type(close_time)}")

            weekday = close_time.weekday()
            if weekday >= 5:
                sigma = 0.0025
            else:
                sigma = 0.0042

            crps = calculate_crps(data, sigma)
            print(data['time_utc'].iloc[-1], crps)

            if start_index >= 0 and start_index + length <= n:
                close_price = float(data["close"].iloc[start_index])
                real_path = data["close"].iloc[start_index: start_index + length].values
                price_paths, average_price = simulate_crypto_price_paths(
                    close_price, 300, 300 * (length - 1), 100, sigma, close_time
                )

                plot_real_path_basic(real_path, price_paths, data, start_index, i + 1, average_price)