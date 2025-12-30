from datetime import datetime, timedelta, time
import pandas as pd
import numpy as np
import requests

def virtual_btc_average_calculation(current_price, close_time, prev_data):
    if isinstance(close_time, pd.Timestamp):
            close_time = close_time.to_pydatetime().replace(tzinfo=None)
    elif not isinstance(close_time, datetime): 
            print(f"Error: close_time must be datetime, got {type(close_time)}")
            return None, None
    
    price = float(current_price)
    weekday = close_time.weekday()

    if weekday == 0:
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
            
        return average_price
    if weekday == 1:
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
        
        return average_price
        
    if weekday == 2:
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

        return average_price
    
    if weekday == 3:
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
        
        return average_price
             
    if weekday == 4:
        result = []

        for i in range(1, len(prev_data)):
            diff = prev_data[i]["avg_price"] - prev_data[i-1]["avg_price"]
            result.append({
                "avg_diff": diff
            })
        result_sum = sum(item['avg_diff'] for item in result[:4])

        prev_flag = []
        for i in range(min(4, len(result))):  # Get first 3 or less if fewer exist
            value = result[i].get('avg_diff', 0)  # Use .get() to avoid KeyError
            if value > 0:
                prev_flag.append(+1)
            else:
                prev_flag.append(-1)

        small_number = sum(
            1 for item in result
            if abs(item["avg_diff"]) < prev_data[0]["avg_price"] * 0.01
        )

        if abs(result[0]["avg_diff"]) >= prev_data[1]["avg_price"] * 0.055:
            if result[0]["avg_diff"] > 0:
                  flag = 1
                  average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.02
            else:
                 flag = -1
                 average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.02
        else:
            if result_sum > 0:
                  flag = 1
            else:
                  flag = -1

            if abs(result[-1]["avg_diff"]) >= prev_data[-1]["avg_price"] * 0.019:
                average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.0085
            else:
                if small_number >=3:
                    if abs(price - prev_data[-1]["avg_price"]) > prev_data[-1]["avg_price"] * 0.02:
                        average_price = price + flag * prev_data[-1]["avg_price"] * 0.03
                    else:
                        average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.02
                else:
                    if abs(price - prev_data[-1]["avg_price"]) > prev_data[-1]["avg_price"] * 0.02:
                        average_price = price + flag * prev_data[-1]["avg_price"] * 0.02
                    elif prev_flag[1] != prev_flag[2]:
                        flag = prev_flag[2]
                        average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.01
                    else:
                        average_price = prev_data[-1]["avg_price"] + flag * prev_data[-1]["avg_price"] * 0.02

        return average_price
    if weekday == 5:
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

        return average_price
    if weekday == 6:
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

        return average_price