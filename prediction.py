import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta, time
from config import EWM_SPAN, EWM_INDEX, WEEKDAY_NAMES
from data_loader import load_or_fetch_data


def calculate_price_differences(csv_file='btc_pyth_1min.csv', output_file='price_differences.csv'):
    """
    Calculate 30-minute average minus daily average price differences.
    """
    df = pd.read_csv(csv_file)
    df['time'] = pd.to_datetime(df['time'])
    
    # Convert to timezone-aware UTC if needed, or remove timezone for easier comparison
    if df['time'].dt.tz is not None:
        # Convert to UTC and then remove timezone for easier datetime manipulation
        df['time'] = df['time'].dt.tz_convert('UTC').dt.tz_localize(None)
    
    # Filter to only 5-minute intervals (:00, :05, :10, :15, :20, :25, :30, :35, :40, :45, :50, :55)
    df['minute'] = df['time'].dt.minute
    df_5min = df[df['minute'].isin([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])].copy()
    df_5min = df_5min.sort_values('time').reset_index(drop=True)
    
    results = []
    
    # Process each row that is at a 30-minute mark (:00 or :30)
    for idx, row in df_5min.iterrows():
        current_time = row['time']
        minute = current_time.minute
        hour = current_time.hour
        
        # Only process at :00 and :30 marks
        if minute not in [0, 30]:
            continue
        
        # Determine the 30-minute window
        if minute == 30:
            # For :30, use :00, :05, :10, :15, :20, :25 of the same hour
            window_start = current_time.replace(minute=0, second=0, microsecond=0)
            window_end = current_time.replace(minute=25, second=0, microsecond=0)
            minute_list = [0, 5, 10, 15, 20, 25]
        else:  # minute == 0
            # For :00, use :30, :35, :40, :45, :50, :55 of the previous hour
            window_start = (current_time - timedelta(hours=1)).replace(minute=30, second=0, microsecond=0)
            window_end = (current_time - timedelta(hours=1)).replace(minute=55, second=0, microsecond=0)
            minute_list = [30, 35, 40, 45, 50, 55]
        
        # Get prices for the 30-minute window
        window_mask = (df_5min['time'] >= window_start) & (df_5min['time'] <= window_end)
        window_prices = df_5min[window_mask]['price']
        
        if len(window_prices) < 6:
            # Not enough data for 30-minute average
            continue
        
        # Calculate 30-minute average
        avg_30min = window_prices.mean()
        
        # Calculate daily average from same time yesterday to current time
        # Daily average is from 24 hours ago (same time yesterday) to current time
        daily_start = current_time - timedelta(days=1)
        daily_end = current_time
        
        # Get all 5-minute prices in the daily range
        daily_mask = (df_5min['time'] >= daily_start) & (df_5min['time'] < daily_end)
        daily_prices = df_5min[daily_mask]['price']
        
        if len(daily_prices) == 0:
            # Not enough data for daily average
            continue
        
        # Calculate daily average
        avg_daily = daily_prices.mean()
        
        # Calculate difference
        difference = avg_30min - avg_daily
        
        results.append({
            'time': current_time,
            '30min_avg': avg_30min,
            'daily_avg': avg_daily,
            'difference': difference
        })
    
    # Create results DataFrame and save to CSV
    if results:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_file, index=False)
        print(f"Saved {len(results)} records to {output_file}")
        return results_df
    else:
        print("No results to save")
        return None


def process_differences_by_weekday(
    input_file='price_differences.csv',
    timezone='UTC'
):
    """
    Process difference values by weekday and 30-minute intervals.
    Creates a 3D structure and calculates EWM12 values.
    """
    # Read the differences CSV
    df = pd.read_csv(input_file)
    df['time'] = pd.to_datetime(df['time'])
    
    # Convert to specified timezone if needed
    if df['time'].dt.tz is None:
        df['time'] = df['time'].dt.tz_localize('UTC')
    df['time'] = df['time'].dt.tz_convert(timezone)
    
    # Extract weekday and time interval (30-minute intervals: 0-47 per day)
    df['weekday'] = df['time'].dt.day_name()
    df['hour'] = df['time'].dt.hour
    df['minute'] = df['time'].dt.minute
    # Map to 30-minute intervals: :00 -> even index (0,2,4...), :30 -> odd index (1,3,5...)
    df['time_interval'] = df['hour'] * 2 + (df['minute'] // 30)
    
    # Create structure: {weekday: {time_interval: [differences]}}
    structure = {}
    for day in WEEKDAY_NAMES:
        structure[day] = {}
        for interval in range(48):  # 48 thirty-minute intervals per day
            structure[day][interval] = []
    
    # Populate the structure with difference values in chronological order
    df_sorted = df.sort_values('time')
    for _, row in df_sorted.iterrows():
        day = row['weekday']
        interval = int(row['time_interval'])
        if day in structure and interval in structure[day]:
            structure[day][interval].append(row['difference'])
    
    # Calculate EWM12 for each day/time_interval combination
    result_matrix = np.zeros((len(WEEKDAY_NAMES), 48))  # 7 days x 48 intervals
    result_matrix[:] = np.nan  # Initialize with NaN
    
    for day_idx, day in enumerate(WEEKDAY_NAMES):
        for interval in range(48):
            values = structure[day][interval]
            
            if len(values) < 3:
                # Not enough data
                continue
            
            # Convert to numpy array for easier manipulation
            values_array = np.array(values)
            
            # Remove the 2 values with largest absolute values
            abs_values = np.abs(values_array)
            if len(values_array) >= 3:
                # Get indices of 2 largest absolute values
                largest_indices = np.argsort(abs_values)[-2:]
                # Remove them
                filtered_values = np.delete(values_array, largest_indices)
            else:
                filtered_values = values_array
            
            if len(filtered_values) == 0:
                continue
            
            # Calculate EWM12 on absolute values
            abs_filtered = np.abs(filtered_values)
            values_series = pd.Series(abs_filtered)
            ewm12 = values_series.ewm(span=EWM_SPAN, adjust=False).mean()
            
            # Get the final value (using EWM_INDEX, which is -2)
            if len(ewm12) >= abs(EWM_INDEX):
                final_value = ewm12.iloc[EWM_INDEX]
                result_matrix[day_idx, interval] = final_value
    
    return result_matrix, WEEKDAY_NAMES


def visualize_heatmap(result_matrix, weekday_names, output_file='difference_heatmap.png'):
    """
    Create a heatmap visualization.
    X-axis: days of week
    Y-axis: time (30-minute intervals)
    Color: EWM12 values
    """
    # Create time labels for y-axis (30-minute intervals)
    time_labels = []
    for hour in range(24):
        for minute in [0, 30]:
            time_labels.append(f"{hour:02d}:{minute:02d}")
    
    # Transpose matrix so that days are columns (x-axis) and times are rows (y-axis)
    # Current shape: (7 days, 48 intervals) -> Need: (48 intervals, 7 days)
    result_matrix_display = result_matrix.T
    
    # Create the heatmap
    plt.figure(figsize=(10, 16))
    
    # Create heatmap using seaborn
    sns.heatmap(
        result_matrix_display,
        xticklabels=weekday_names,
        yticklabels=time_labels,
        cmap='RdYlGn',
        center=0,
        cbar_kws={'label': 'EWM12 of |Difference|'},
        annot=False,
        fmt='.2f',
        linewidths=0.5,
        linecolor='gray'
    )
    
    plt.title('Price Difference EWM12 by Weekday and Time', fontsize=16, fontweight='bold')
    plt.xlabel('Day of Week', fontsize=12)
    plt.ylabel('Time (30-minute intervals)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Heatmap saved to {output_file}")
    plt.show()


def main():
    # First, calculate price differences if needed
    data = load_or_fetch_data('BTC', fetch_new_data=True)
    calculate_price_differences()
    
    # # Process differences by weekday and create visualization
    # result_matrix, weekday_names = process_differences_by_weekday()
    # visualize_heatmap(result_matrix, weekday_names)


if __name__ == "__main__":
    main()
