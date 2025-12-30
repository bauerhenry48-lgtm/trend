"""Data loading and fetching functionality."""

from datetime import datetime, timedelta, timezone
import pandas as pd
from price_data_provider import PriceDataProvider
from utils import convert_to_datetime


def load_or_fetch_data(symbol: str, fetch_new_data: bool = True) -> pd.DataFrame:
    """
    Load data from CSV or fetch new data from API.
    If CSV exists and fetch_new_data is True, removes the last row (may be incomplete)
    and fetches new data starting from the second-to-last row's timestamp.
    """
    csv_file = f'{symbol.lower()}_pyth_1min.csv'
    
    if fetch_new_data:
        existing_data = None
        fetch_start_time = None
        
        # Try to load existing CSV file
        try:
            # print("[LOAD] LOADING EXISTING DATA FROM CSV...")
            existing_data = pd.read_csv(csv_file, index_col=0, parse_dates=True)
            
            if len(existing_data) > 0:
                # Remove the last row (may be incomplete)
                last_timestamp = existing_data.index[-1]
                existing_data = existing_data.iloc[:-1].copy()
                
                # Use the deleted row's timestamp as the starting point for fetching
                fetch_start_time = convert_to_datetime(last_timestamp)
                print(f"[FETCH] Will fetch new data starting from {fetch_start_time}")
            else:
                print("++++++ No existing data remaining, will fetch from beginning")
        except FileNotFoundError:
            print("++++++ No existing CSV file found, will fetch all data from beginning")
        
        print("++++++ FETCHING NEW DATA FROM API...")
        
        # Determine fetch strategy
        if fetch_start_time is not None:
            new_data = _fetch_incremental_data(symbol, None, fetch_start_time)
            # Append only new data to CSV
            if new_data is not None and len(new_data) > 0:
                _append_to_csv(csv_file, new_data, existing_data)
                # Combine for return
                if existing_data is not None and len(existing_data) > 0:
                    price_data = pd.concat([existing_data, new_data])
                    price_data = price_data[~price_data.index.duplicated(keep='last')]
                    price_data = price_data.sort_index()
                else:
                    price_data = new_data
            else:
                price_data = existing_data if existing_data is not None else pd.DataFrame(columns=['price'])
        else:
            price_data = _fetch_full_historical_data(symbol)
            # Save full data to CSV (first time or full refresh)
            price_data.to_csv(csv_file)
            print(f"[SAVE] Saved {len(price_data)} data points to {csv_file}")
    
    else:
        print("[LOAD] LOADING EXISTING DATA FROM CSV...")
        price_data = pd.read_csv(csv_file, index_col=0, parse_dates=True)
    
    return price_data


def _append_to_csv(csv_file: str, new_data: pd.DataFrame, existing_data: pd.DataFrame = None):
    """
    Append only new data rows to CSV file and remove oldest data to maintain file size.
    Uses a safe approach: first appends new data, then trims oldest rows using atomic file operations.
    """
    import os
    import tempfile
    
    if new_data is None or len(new_data) == 0:
        return
    
    # Step 1: Check for existing timestamps by reading just the index (faster)
    file_exists = os.path.exists(csv_file)
    existing_timestamps = set()
    
    if file_exists:
        try:
            # Read only index to check for duplicates (faster than reading full data)
            file_data = pd.read_csv(csv_file, index_col=0, parse_dates=True, usecols=[0])
            existing_timestamps = set(file_data.index)
        except (FileNotFoundError, pd.errors.EmptyDataError):
            file_exists = False
    
    # Filter out rows that already exist in the CSV file
    new_data_filtered = new_data[~new_data.index.isin(existing_timestamps)]
    
    if len(new_data_filtered) == 0:
        print(f"[SAVE] No new data to append (all duplicates)")
        return
    
    # Sort by index before appending
    new_data_filtered = new_data_filtered.sort_index()
    num_new_rows = len(new_data_filtered)
    
    # Step 2: Append new data directly to CSV file (fast, safe operation)
    new_data_filtered.to_csv(csv_file, mode='a', header=not file_exists)
    print(f"[SAVE] Appended {num_new_rows} new data points to {csv_file}")
    
    # Step 3: Trim oldest rows if needed (only if file existed before)
    if file_exists and num_new_rows > 0:
        try:
            # Read the entire file (now includes appended data)
            full_data = pd.read_csv(csv_file, index_col=0, parse_dates=True)
            
            # Remove duplicates (keep last occurrence)
            full_data = full_data[~full_data.index.duplicated(keep='last')]
            full_data = full_data.sort_index()
            
            # Check if trimming is needed
            if len(full_data) > num_new_rows:
                # Keep all rows except the oldest num_new_rows
                trimmed_data = full_data.iloc[num_new_rows:].copy()
                
                # Write to temporary file first (atomic operation)
                temp_file = csv_file + '.tmp'
                trimmed_data.to_csv(temp_file)
                
                # Atomically replace the original file (safer than direct overwrite)
                if os.name == 'nt':  # Windows
                    # On Windows, need to remove original first, then rename
                    os.replace(temp_file, csv_file)
                else:  # Unix-like
                    # On Unix, rename is atomic
                    os.rename(temp_file, csv_file)
                
                # print(f"[SAVE] Removed {num_new_rows} oldest data points to maintain file size (total: {len(trimmed_data)} rows)")
            elif len(full_data) < len(existing_timestamps) + num_new_rows:
                # Duplicates were found and removed, update file
                temp_file = csv_file + '.tmp'
                full_data.to_csv(temp_file)
                if os.name == 'nt':
                    os.replace(temp_file, csv_file)
                else:
                    os.rename(temp_file, csv_file)
                print(f"[SAVE] Removed duplicates, total: {len(full_data)} rows")
        except Exception as e:
            print(f"[ERROR] Failed to trim oldest rows: {e}. New data was still appended.")
            # New data is safe in the file, trimming can be done later


def _fetch_incremental_data(symbol: str, existing_data: pd.DataFrame, fetch_start_time: datetime) -> pd.DataFrame:
    """Fetch incremental data from fetch_start_time to now."""
    current_dt = datetime.now(timezone.utc)
    seconds_to_fetch = (current_dt - fetch_start_time).total_seconds()
    
    if seconds_to_fetch <= 0:
        print("[FETCH] No new data to fetch (data is up to date)")
        return existing_data if existing_data is not None else pd.DataFrame(columns=['price'])
    
    data_list = []
    if existing_data is not None:
        data_list.append(existing_data)
    
    # Fetch in chunks of 1 day (86400 seconds = 1 day)
    seconds_processed = 0
    chunk_seconds = 86400  # 1 day in seconds
    
    while seconds_processed < seconds_to_fetch - 300:
        chunk_size = min(chunk_seconds, int((seconds_to_fetch - seconds_processed) // 300) * 300)
        start_time_dt = fetch_start_time + timedelta(seconds=seconds_processed)
        start_time = start_time_dt.isoformat()
        
        # Fetch data for this chunk
        time_length = int(chunk_size)
        price_data = PriceDataProvider().fetch_data(symbol, start_time, time_length, transformed=True)
        print(f"Fetched {len(price_data)} data points starting from {start_time}")
        
        if price_data:
            price_data_df = pd.DataFrame(price_data)
            price_data_df['time'] = pd.to_datetime(price_data_df['time'])
            price_data_df.set_index('time', inplace=True)
            # Filter to only include data after fetch_start_dt to avoid duplicates
            price_data_df = price_data_df[price_data_df.index >= fetch_start_time]
            if len(price_data_df) > 0:
                data_list.append(price_data_df)
        
        seconds_processed += chunk_size
    
    if data_list:
        price_data = pd.concat(data_list)
        price_data.index = pd.to_datetime(price_data.index)
        price_data = price_data[~price_data.index.duplicated(keep='last')]
        price_data = price_data.sort_index()
        return price_data
    else:
        return existing_data if existing_data is not None else pd.DataFrame(columns=['price'])


def _fetch_full_historical_data(symbol: str) -> pd.DataFrame:
    """Fetch all historical data (original logic)."""
    days_back = list(range(90, 0, -1))
    data_list = []
    current_dt = datetime.now(timezone.utc)
    unix_time = current_dt.timestamp() - current_dt.timestamp() % 300

    for days in days_back:
        start_time = (datetime.fromtimestamp(unix_time, tz=timezone.utc) - timedelta(days)).isoformat()
        price_data = PriceDataProvider().fetch_data(symbol, start_time, 86400, transformed=True)
        print(f"Fetched {len(price_data)} data points starting from {start_time}")
        
        if price_data:
            price_data_df = pd.DataFrame(price_data)
            price_data_df['time'] = pd.to_datetime(price_data_df['time'])
            price_data_df.set_index('time', inplace=True)
            data_list.append(price_data_df)

    if data_list:
        price_data = pd.concat(data_list)
        price_data.index = pd.to_datetime(price_data.index)
        price_data = price_data[~price_data.index.duplicated(keep='last')]
        price_data = price_data.sort_index()
        return price_data
    else:
        return pd.DataFrame(columns=['price'])
