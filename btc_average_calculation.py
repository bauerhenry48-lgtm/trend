from datetime import datetime, timedelta, time
import pandas as pd
import numpy as np
import requests

from virtural_btc_average_calculation import virtual_btc_average_calculation

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

def convert_to_next_day_midnight(datetime_obj):
    if isinstance(datetime_obj, pd.Timestamp):
        datetime_obj = datetime_obj.to_pydatetime()
    
    # Add one day to the date
    next_day = datetime_obj + timedelta(days=1)
    
    # Set time to 00:00:00 (midnight)
    next_day_midnight = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
    
    return next_day_midnight

def btc_average_calculation(current_price, close_time):
    if isinstance(close_time, pd.Timestamp):
            close_time = close_time.to_pydatetime().replace(tzinfo=None)
    elif not isinstance(close_time, datetime): 
            print(f"Error: close_time must be datetime, got {type(close_time)}")
            return None, None
    
    price = float(current_price)
    weekday = close_time.weekday()

    close_time_midnight = pd.to_datetime(close_time).normalize() 
    pass_time = (close_time - close_time_midnight).total_seconds() /60
    if pass_time / 60 <= 14:
        pass_data = fetch_ohlcv(close_time, symbol='BTC', interval='Min5', days_history=pass_time/1440, days_offset=0, limit=600)
    else:
        pass_data = fetch_ohlcv(close_time, symbol='BTC', interval='Min5', days_history=840/1440, days_offset=0, limit=600)

    if weekday == 0:
        prev_data = []
        for days_offset in range(7, -1, -1):
            df = fetch_ohlcv(
                close_time_midnight,
                symbol='BTC',
                days_history=1,
                days_offset=days_offset
            )

            if not df.empty:
                day = df.index[0].date()
                avg_price = df['close'].mean()
                prev_data.append({
                    "date": day,
                    "avg_price": avg_price
                })

        big_num = 0                

        result = []
        for i in range(1, len(prev_data)):
            diff = prev_data[i]["avg_price"] - prev_data[i-1]["avg_price"]
            result.append({
                "avg_diff": diff
            })
        total_diff = sum(result[i]["avg_diff"] for i in range(5))

        for i in range(0, len(result)):
            if abs(result[i]["avg_diff"]) >= prev_data[1]["avg_price"] * 0.035:
                big_num = 1  # Set to 1 when condition is met
                break  # Exit immediately
        else:
            big_num = -1 
        
        prev_flag = []
        last_3_items = result[-3:]
        for item in last_3_items:
            diff_value = item["avg_diff"]  
            if diff_value > 0:
                prev_flag.append(1)
            else:
                prev_flag.append(-1)
        total_sum = sum(prev_flag)

        big_num_last3 = 0

        for i in range(0, len(last_3_items)):
            if abs(last_3_items[i]["avg_diff"]) >= prev_data[1]["avg_price"] * 0.011:
                big_num_last3 = 1  # Set to 1 when condition is met
                break  # Exit immediately
        else:
            big_num_last3 = -1 


        if total_diff > 0:
             week_flag = -1
        else:
             week_flag = 1

        last = result[-1]["avg_diff"]

        if last > 0:
             sunday_flag = 1
        else:
             sunday_flag = -1

        small_number = sum(
            1 for item in result
            if abs(item["avg_diff"]) < prev_data[-1]["avg_price"] / 100
        )

        if week_flag == sunday_flag:
             flag = sunday_flag
        else:
            if total_sum == 3:
                flag = 1
            elif total_sum == -3:
                flag = -1
            else:
                 flag = week_flag

        if abs(result[4]["avg_diff"]) >= prev_data[4]["avg_price"] * 0.045:
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.009
        elif total_sum == 3 and big_num_last3 == -1:
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.009
        elif total_sum == -3 and big_num_last3 == -1:
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.009
        elif big_num_last3 == -1:
            if 4 <= small_number < 6 and big_num == 1:
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.03
            elif small_number >= 6 and big_num == 1:
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.06
            else:
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.01
        elif abs(result[5]["avg_diff"]) >= prev_data[5]["avg_price"] * 0.012:
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.0145
        else:
             average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.01
            
        total_pass_price = 0
        close_prices = pass_data["close"].values
        for i in range(1, len(pass_data)):
            total_pass_price += (average_price - close_prices[i]) / (24 * 12)
        real_average_price = average_price + total_pass_price
        
        if pass_time / 60 <= 14:
            return real_average_price
        else:
            last_time = pass_time - 840
            tomorrow_time = convert_to_next_day_midnight(close_time)
            prev_data.append({
                "date": close_time.date(),
                "avg_price": average_price
            })
            prev_data = prev_data[-5:]
            predict_average = virtual_btc_average_calculation(average_price, tomorrow_time, prev_data)
            virtual_average_price = real_average_price + (predict_average - real_average_price) / 600 * last_time
            return virtual_average_price
    if weekday == 1:
         
        prev_data = []

        for days_offset in range(4, -1, -1):
            df = fetch_ohlcv(
                close_time_midnight,
                symbol='BTC',
                days_history=1,
                days_offset=days_offset
            )

            if not df.empty:
                day = df.index[0].date()
                avg_price = df['close'].mean()
                prev_data.append({
                    "date": day,
                    "avg_price": avg_price
                })

        small_num = 0
        result = []

        for i in range(1, len(prev_data)):
            diff = prev_data[i]["avg_price"] - prev_data[i-1]["avg_price"]
            result.append({
                "avg_diff": diff
            })

        for i in range(0, len(result)):
            if abs(result[i]["avg_diff"]) <= prev_data[1]["avg_price"] * 0.011:
                small_num += 1  

        prev_flag = []
        last_4_items = result[-4:]
        for item in last_4_items:
            diff_value = item["avg_diff"]  
            if diff_value > 0:
                prev_flag.append(1)
            else:
                prev_flag.append(-1)
        total_sum = sum(prev_flag)

        last = result[-1]["avg_diff"]

        if last > 0:
             prev_flag = 1
        else:
             prev_flag = -1

        if (total_sum == 4 or total_sum == -4) and small_num == 4:
            flag = -prev_flag
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.015
        elif abs(last) >= prev_data[-1]["avg_price"] * 0.027:
             flag = -prev_flag
             average_price = prev_data[-1]["avg_price"] + flag * abs(last) * 0.45
        elif prev_flag == -1 and prev_data[-1]["avg_price"] * 0.012 <= abs(last) < prev_data[-1]["avg_price"] * 0.027:
            flag = prev_flag
            average_price = prev_data[-1]["avg_price"] + flag * abs(last) * 1.25
        elif prev_flag == -1 and abs(last) < prev_data[-1]["avg_price"] * 0.012:
            flag = prev_flag
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.025
        elif prev_flag == 1 and (0.012 * prev_data[-1]["avg_price"] <= abs(result[-1]["avg_diff"]) < 0.027 * prev_data[-1]["avg_price"] or 
                                 0.012 * prev_data[-2]["avg_price"] <= abs(result[-2]["avg_diff"]) < 0.027 * prev_data[-2]["avg_price"]):
            flag = prev_flag 
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.003
        else:
             flag = prev_flag
             average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.012
        
        total_pass_price = 0
        close_prices = pass_data["close"].values
        for i in range(1, len(pass_data)):
            total_pass_price += (average_price - close_prices[i]) / (24 * 12)
        real_average_price = average_price + total_pass_price
        
        if pass_time / 60 <= 14:
            return real_average_price
        else:
            last_time = pass_time - 840
            tomorrow_time = convert_to_next_day_midnight(close_time)
            prev_data.append({
                "date": close_time.date(),
                "avg_price": average_price
            })
            prev_data = prev_data[-5:]
            predict_average = virtual_btc_average_calculation(average_price, tomorrow_time, prev_data)
            virtual_average_price = real_average_price + (predict_average - real_average_price) / 600 * last_time
            return virtual_average_price
        
    if weekday == 2:
        prev_data = []

        for days_offset in range(4, -1, -1):
            df = fetch_ohlcv(
                close_time_midnight,
                symbol='BTC',
                days_history=1,
                days_offset=days_offset
            )

            if not df.empty:
                day = df.index[0].date()
                avg_price = df['close'].mean()
                prev_data.append({
                    "date": day,
                    "avg_price": avg_price
                })

        result = []
        medium_num = 0

        for i in range(1, len(prev_data)):
            diff = prev_data[i]["avg_price"] - prev_data[i-1]["avg_price"]
            result.append({
                "avg_diff": diff
            })

        prev_flag = []
        last_4_items = result[-4:]
        for item in last_4_items:
            diff_value = item["avg_diff"]  
            if diff_value > 0:
                prev_flag.append(1)
            else:
                prev_flag.append(-1)
        total_sum = sum(prev_flag)

        for i in range(0, len(result)):
            if abs(result[i]["avg_diff"]) >= prev_data[1]["avg_price"] * 0.012:
                medium_num += 1  

        last = result[-1]["avg_diff"]

        if total_sum == 4:
            if medium_num >= 3:
                flag = -1
            else:
                if last > 0:
                    flag = 1
                else:
                    flag = -1
        elif total_sum == -4:
            if medium_num >= 3:
                flag =1
            else:
                if last > 0:
                    flag = 1
                else:
                    flag = -1
        else:
            if last > 0:
                flag = 1
            else:
                flag = -1

        if abs(result[-2]["avg_diff"]) >= prev_data[1]["avg_price"] * 0.027:
             average_price = prev_data[-1]["avg_price"] + flag * abs(result[-2]["avg_diff"]) * 0.65
        elif abs(result[-1]["avg_diff"]) >= prev_data[-1]["avg_price"] * 0.012:
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.005
        elif abs(result[-1]["avg_diff"]) < prev_data[-1]["avg_price"] * 0.012 and abs(result[-2]["avg_diff"]) > prev_data[-1]["avg_price"] * 0.012:
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.009
        else:
             average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.005

        total_pass_price = 0
        close_prices = pass_data["close"].values
        for i in range(1, len(pass_data)):
            total_pass_price += (average_price - close_prices[i]) / (24 * 12)
        real_average_price = average_price + total_pass_price
        
        if pass_time / 60 <= 14:
            return real_average_price
        else:
            last_time = pass_time - 840
            tomorrow_time = convert_to_next_day_midnight(close_time)
            prev_data.append({
                "date": close_time.date(),
                "avg_price": average_price
            })
            prev_data = prev_data[-4:]
            predict_average = virtual_btc_average_calculation(average_price, tomorrow_time, prev_data)
            virtual_average_price = real_average_price + (predict_average - real_average_price) / 600 * last_time
            return virtual_average_price
    
    if weekday == 3:
        prev_data = []

        for days_offset in range(3, -1, -1):
            df = fetch_ohlcv(
                close_time_midnight,
                symbol='BTC',
                days_history=1,
                days_offset=days_offset
            )

            if not df.empty: 
                day = df.index[0].date()
                avg_price = df['close'].mean()
                prev_data.append({
                    "date": day,
                    "avg_price": avg_price
                })
        
        result = []

        for i in range(1, len(prev_data)):
            diff = prev_data[i]["avg_price"] - prev_data[i-1]["avg_price"]
            result.append({
                "avg_diff": diff
            })
        result_sum = sum(item['avg_diff'] for item in result[:3])

        prev_flag = []
        for i in range(min(3, len(result))):  # Get first 3 or less if fewer exist
            value = result[i].get('avg_diff', 0)  # Use .get() to avoid KeyError
            if value > 0:
                prev_flag.append(+1)
            else:
                prev_flag.append(-1)

        total_sum = sum(prev_flag)
        
        big_number = sum(
            1 for item in result
            if abs(item["avg_diff"]) >= prev_data[0]["avg_price"] * 0.022
        )
        small_number = sum(
            1 for item in result
            if abs(item["avg_diff"]) < prev_data[0]["avg_price"] * 0.01
        )
        medium_number = sum(
            1 for item in result
            if (prev_data[0]["avg_price"] * 0.01) < abs(item["avg_diff"]) < (prev_data[0]["avg_price"] * 0.022)
        )

        def sum_of_two_small(result):
            if len(result) != 3:
                raise ValueError("Input must contain exactly 3 numbers")
            
            diffs = [item["avg_diff"] for item in result[:3]]
            # Filter numbers with abs < 1000
            small_abs = [d for d in diffs if abs(d) < prev_data[0]["avg_price"] * 0.01]
            
            if len(small_abs) == 2:
                return sum(small_abs)
            else:
                return None

        if big_number >= 2:
            if total_sum == 3:
                  flag = -1
                  average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.002
            elif total_sum == -3:
                 flag = 1
                 average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.002
            else:
                if result_sum > 0:
                      flag = 1
                      average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.002
                else:
                     flag = -1
                     average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.002
        elif small_number == 3:
             if result_sum > 0:
                  flag = 1
                  average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.037
             if result_sum < 0:
                  flag = -1
                  average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.037
        elif small_number == 2:
            num = sum_of_two_small(result)
            if num > 0:
                flag = 1
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.006
            else:
                flag = -1
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.006
        elif medium_number >= 2:
            if total_sum == 3:
                flag = -1
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.019
            elif total_sum == -3:
                flag = 1
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.019
            else:
                if prev_flag[1] != prev_flag[2]:
                    flag = prev_flag[1]
                    average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.003
                elif result_sum > 0:
                    flag = -1
                    average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.015
                else:
                    flag = 1
                    average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.015
        else:
             if result[-1]["avg_diff"] > 0:
                  flag = 1
                  average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.005
             else:
                  flag = -1
                  average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.005
        
        total_pass_price = 0
        close_prices = pass_data["close"].values
        for i in range(1, len(pass_data)):
            total_pass_price += (average_price - close_prices[i]) / (24 * 12)
        real_average_price = average_price + total_pass_price
        
        if pass_time / 60 <= 14:
            return real_average_price
        else:
            last_time = pass_time - 840
            tomorrow_time = convert_to_next_day_midnight(close_time)
            prev_data.append({
                "date": close_time.date(),
                "avg_price": average_price
            })
            prev_data = prev_data[-5:]
            predict_average = virtual_btc_average_calculation(average_price, tomorrow_time, prev_data)
            virtual_average_price = real_average_price + (predict_average - real_average_price) / 600 * last_time
            return virtual_average_price
             
    if weekday == 4:
        prev_data = []

        for days_offset in range(4, -1, -1):
            df = fetch_ohlcv(
                close_time_midnight,
                symbol='BTC',
                days_history=1,
                days_offset=days_offset
            )

            if not df.empty: 
                day = df.index[0].date()
                avg_price = df['close'].mean()
                prev_data.append({
                    "date": day,
                    "avg_price": avg_price
                })
        
        result = []

        for i in range(1, len(prev_data)):
            diff = prev_data[i]["avg_price"] - prev_data[i-1]["avg_price"]
            result.append({
                "avg_diff": diff
            })
        result_sum = sum(item['avg_diff'] for item in result[:4])

        diffs = [r["avg_diff"] for r in result]

        # Sort by absolute value (largest first) and take first two
        sorted_by_abs = sorted(diffs, key=lambda x: abs(x), reverse=True)
        big2_array = sorted_by_abs[:2]

        prev2_flag = []
        for i in range(min(2, len(big2_array))):  # Get first 3 or less if fewer exist
            value = big2_array[i]  # Use .get() to avoid KeyError
            if value > 0:
                prev2_flag.append(+1)
            else:
                prev2_flag.append(-1)
        total2_sum = sum(prev2_flag)

        prev_flag = []
        for i in range(min(4, len(result))):  # Get first 3 or less if fewer exist
            value = result[i].get('avg_diff', 0)  # Use .get() to avoid KeyError
            if value > 0:
                prev_flag.append(+1)
            else:
                prev_flag.append(-1)

        total_sum = sum(prev_flag)
        
        big_number = sum(
            1 for item in result
            if abs(item["avg_diff"]) >= prev_data[0]["avg_price"] * 0.022
        )
        small_number = sum(
            1 for item in result
            if abs(item["avg_diff"]) < prev_data[0]["avg_price"] * 0.011
        )
        medium_number = sum(
            1 for item in result
            if (prev_data[0]["avg_price"] * 0.011) <= abs(item["avg_diff"]) < (prev_data[0]["avg_price"] * 0.022)
        )

        if total_sum == 4 or total_sum == -4:
            if small_number == 3 and big_number == 1 and abs(result[-1]["avg_diff"]) < 0.01 * prev_data[-1]["avg_price"]:
                flag = prev_flag[-1]
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.065
            else:
                flag = prev_flag[-1]
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.003
        elif big_number >= 2:
            if total2_sum == 2 or total2_sum == -2:
                flag = prev2_flag[-1]
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.01
            else:
                if abs(big2_array[0]) > abs(big2_array[1]):
                    flag = prev2_flag[0]
                    average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.02
                else:
                    flag = prev2_flag[1]
                    average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.02
        elif medium_number == 3 and big_number == 1 and abs(result[-1]["avg_diff"]) < 0.022 * prev_data[-1]["avg_price"]:
            if result_sum > 0:
                flag = 1
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.047
            else:
                flag = -1
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.047
        elif small_number == 3 and medium_number == 1:
            if result_sum > 0:
                flag = -1
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.004
            else:
                flag = 1
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.004
        elif prev_flag[1] != prev_flag[2]:
            flag = prev_flag[2]
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.009
        else:
            if result_sum > 0:
                flag = 1
                average_price = average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.01
            else:
                flag = -1
                average_price = average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.01
        
        total_pass_price = 0
        close_prices = pass_data["close"].values
        for i in range(1, len(pass_data)):
            total_pass_price += (average_price - close_prices[i]) / (24 * 12)
        real_average_price = average_price + total_pass_price
        
        if pass_time / 60 <= 14:
            return real_average_price
        else:
            last_time = pass_time - 840
            tomorrow_time = convert_to_next_day_midnight(close_time)
            prev_data.append({
                "date": close_time.date(),
                "avg_price": average_price
            })
            prev_data = prev_data[-6:]
            predict_average = virtual_btc_average_calculation(average_price, tomorrow_time, prev_data)
            virtual_average_price = real_average_price + (predict_average - real_average_price) / 600 * last_time
            return virtual_average_price
    if weekday == 5:
        prev_data = []

        for days_offset in range(5, -1, -1):
            df = fetch_ohlcv(
                close_time_midnight,
                symbol='BTC',
                days_history=1,
                days_offset=days_offset
            )

            if not df.empty: 
                day = df.index[0].date()
                avg_price = df['close'].mean()
                prev_data.append({
                    "date": day,
                    "avg_price": avg_price
                })
        
        result = []

        for i in range(1, len(prev_data)):
            diff = prev_data[i]["avg_price"] - prev_data[i-1]["avg_price"]
            result.append({
                "avg_diff": diff
            })

        result_sum = sum(item['avg_diff'] for item in result[:5])
        if abs(result[-1]["avg_diff"]) >= 0.06 * prev_data[-1]["avg_price"]:
            if result[-1]["avg_diff"] > 0:
                flag = 1
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.003
            else:
                flag = -1 
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.003
        elif 0.019 * prev_data[-1]["avg_price"] <= abs(result[-1]["avg_diff"]) < 0.06 * prev_data[-1]["avg_price"]:
            if result[-1]["avg_diff"] > 0:
                  flag = 1
                  average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.013
            else:
                flag = -1 
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.013
        else:
            if result_sum > 0:
                flag = -1
                if 0.014 * prev_data[-2]["avg_price"] <= abs(result[-2]["avg_diff"]) < 0.02 * prev_data[-2]["avg_price"]:
                    average_price = prev_data[-1]["avg_price"] + flag * abs(result[-2]["avg_diff"]) * 0.75
                else:
                    average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.0085
            else:
                flag = 1
                if 0.014 * prev_data[-2]["avg_price"] <= abs(result[-2]["avg_diff"]) < 0.02 * prev_data[-2]["avg_price"]:
                    average_price = prev_data[-1]["avg_price"] + flag * abs(result[-2]["avg_diff"]) * 0.75
                else:
                    average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.0085

        total_pass_price = 0
        close_prices = pass_data["close"].values
        for i in range(1, len(pass_data)):
            total_pass_price += (average_price - close_prices[i]) / (24 * 12)
        real_average_price = average_price + total_pass_price
        
        if pass_time / 60 <= 14:
            return real_average_price
        else:
            last_time = pass_time - 840
            tomorrow_time = convert_to_next_day_midnight(close_time)
            prev_data.append({
                "date": close_time.date(),
                "avg_price": average_price
            })
            prev_data = prev_data[-7:]
            predict_average = virtual_btc_average_calculation(average_price, tomorrow_time, prev_data)
            virtual_average_price = real_average_price + (predict_average - real_average_price) / 600 * last_time
            return virtual_average_price
    if weekday == 6:
        prev_data = []

        for days_offset in range(6, -1, -1):
            df = fetch_ohlcv(
                close_time_midnight,
                symbol='BTC',
                days_history=1,
                days_offset=days_offset
            )

            if not df.empty: 
                day = df.index[0].date()
                avg_price = df['close'].mean()
                prev_data.append({
                    "date": day,
                    "avg_price": avg_price
                })
        
        result = []

        for i in range(1, len(prev_data)):
            diff = prev_data[i]["avg_price"] - prev_data[i-1]["avg_price"]
            result.append({

                "avg_diff": diff
            })

        result_sum = sum(item['avg_diff'] for item in result[:5])

        prev_flag = []
        for i in range(0, len(result)):  # Get first 3 or less if fewer exist
            value = result[i]["avg_diff"]  # Use .get() to avoid KeyError
            if value > 0:
                prev_flag.append(+1)
            else:
                prev_flag.append(-1)
        prev_flag_sum = sum(prev_flag)

        if prev_flag_sum >= 4:
            flag = 1
        elif prev_flag_sum <= -4:
            flag = -1
        elif result_sum > 0:
            flag = -1
        else:
            flag = 1

        if abs(result[-2]["avg_diff"]) >= 0.06 * prev_data[-2]["avg_price"]:
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.025
        else:
            average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.005 

        total_pass_price = 0
        close_prices = pass_data["close"].values
        for i in range(1, len(pass_data)):
            total_pass_price += (average_price - close_prices[i]) / (24 * 12)
        real_average_price = average_price + total_pass_price
        
        if pass_time / 60 <= 14:
            return real_average_price
        else:
            last_time = pass_time - 840
            tomorrow_time = convert_to_next_day_midnight(close_time)
            prev_data.append({
                "date": close_time.date(),                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
                "avg_price": average_price
            })
            prev_data = prev_data[-8:]
            predict_average = virtual_btc_average_calculation(average_price, tomorrow_time, prev_data)
            virtual_average_price = real_average_price + (predict_average - real_average_price) / 600 * last_time
            return virtual_average_price
         

                       