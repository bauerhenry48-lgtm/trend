import logging
import requests
from datetime import datetime, timezone, timedelta
from tenacity import (
    before_log,
    retry,
    stop_after_attempt,
    wait_random_exponential,
)


from helpers import from_iso_to_unix_time

# Pyth API benchmarks doc: https://benchmarks.pyth.network/docs
# get the list of stocks supported by pyth: https://benchmarks.pyth.network/v1/shims/tradingview/symbol_info?group=pyth_stock
# get the list of crypto supported by pyth: https://benchmarks.pyth.network/v1/shims/tradingview/symbol_info?group=pyth_crypto
# get the ticket: https://benchmarks.pyth.network/v1/shims/tradingview/symbols?symbol=Metal.XAU/USD


class PriceDataProvider:
    BASE_URL = "https://benchmarks.pyth.network/v1/shims/tradingview/history"

    TOKEN_MAP = {
        "BTC": "Crypto.BTC/USD",
        "ETH": "Crypto.ETH/USD",
        "XAU": "Metal.XAU/USD",
        "SOL": "Crypto.SOL/USD",
    }

    _logger = logging.getLogger(__name__)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=7),
        reraise=True,
        before=before_log(_logger, logging.DEBUG),
    )
    def fetch_data(
        self, token: str, start_time: str, time_length: int, transformed=True
    ):
        """
        Fetch real prices data from an external REST service.
        Returns an array of time points with prices.

        :return: List of dictionaries with 'time' and 'price' keys.
        """

        start_time_int = from_iso_to_unix_time(start_time)
        end_time_int = start_time_int + time_length

        params = {
            "symbol": self._get_token_mapping(token),
            "resolution": 1,
            "from": start_time_int,
            "to": end_time_int,
        }

        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()

        if not transformed:
            return data

        transformed_data = self._transform_data(data, start_time_int)

        # Apply XAU market break handling if needed
        if token == 'XAU' and len(transformed_data) > 0:
            transformed_data = self._handle_xau_market_break(
                transformed_data, start_time_int, end_time_int
            )

        return transformed_data

    @staticmethod
    def _transform_data(data, start_time) -> list[dict]:
        if data is None or len(data) == 0:
            return []

        timestamps = data["t"]
        close_prices = data["c"]

        transformed_data = []

        for t, c in zip(timestamps, close_prices):
            if (
                t >= start_time and (t - start_time) % 60 == 0
            ):  # 300s = 5 minutes
                transformed_data.append(
                    {
                        "time": datetime.fromtimestamp(
                            t, timezone.utc
                        ).isoformat(),
                        "price": float(c),
                    }
                )

        return transformed_data

    @staticmethod
    def _get_token_mapping(token: str) -> str:
        """
        Retrieve the mapped value for a given token.
        If the token is not in the map, raise an exception or return None.
        """
        if token in PriceDataProvider.TOKEN_MAP:
            return PriceDataProvider.TOKEN_MAP[token]
        else:
            raise ValueError(f"Token '{token}' is not supported.")

    @staticmethod
    def _is_xau_market_break(dt: datetime) -> bool:
        """
        Check if a datetime falls during XAU market break period.
        Break: 10:01 PM - 10:58 PM (Mon-Thu), 10:01 PM Fri - 10:58 PM Sun
        """
        hour = dt.hour
        minute = dt.minute
        weekday = dt.weekday()  # 0=Monday, 4=Friday, 6=Sunday
        
        if weekday < 4:  # Monday-Thursday
            return hour == 22 and 1 <= minute <= 58
        elif weekday == 4:  # Friday
            return hour >= 22 and minute >= 1
        elif weekday == 5:  # Saturday
            return True
        elif weekday == 6:  # Sunday
            return hour < 22 or (hour == 22 and minute <= 58)
        
        return False

    @staticmethod
    def _get_xau_market_start_point(dt: datetime) -> datetime:
        """
        Get the market start point for XAU given a datetime.
        For Mon-Thu: returns 22:59 (10:59 PM) of the same day
        For Fri-Sun: returns 22:59 (10:59 PM) of the following Sunday
        """
        weekday = dt.weekday()
        
        if weekday < 4:  # Monday-Thursday
            return dt.replace(hour=22, minute=59, second=0, microsecond=0)
        elif weekday >= 4:  # Friday, Saturday or Sunday
            sunday = dt + timedelta(days=6-weekday)
            return sunday.replace(hour=22, minute=59, second=0, microsecond=0)
        
        return dt

    def _get_xau_market_end_point(dt: datetime) -> datetime:
        """
        Get the market end point for XAU given a datetime.
        For Mon-Thu: returns 22:00 (10:00 PM) of the same day
        For Fri-Sun: returns 22:00 (10:00 PM) of the following Sunday
        """
        weekday = dt.weekday()

        if weekday < 4:  # Monday-Thursday
            return dt.replace(hour=22, minute=0, second=0, microsecond=0)
        elif weekday >= 4:  # Friday, Saturday or Sunday
            friday = dt - timedelta(days=weekday-4)
            return friday.replace(hour=22, minute=0, second=0, microsecond=0)
        
        return dt.replace(hour=22, minute=0, second=0, microsecond=0)

    @staticmethod
    def _has_market_break_between(transformed_data: list[dict]) -> tuple[bool, str]:
        """
        Check if there's a market break period in the transformed data.
        Returns a tuple with a boolean indicating if there's a market break and the timestamp of the market break.
        """
        for item in transformed_data:
            if PriceDataProvider._is_xau_market_break(datetime.fromisoformat(item['time'].replace('Z', '+00:00'))):
                return True, item['time']       
        return False, None

    @staticmethod
    def _handle_xau_market_break(
        transformed_data: list[dict], start_time_int: int, end_time_int: int
    ) -> list[dict]:
        """
        Handle XAU market break logic when end_time_int doesn't match last data item.
        
        - If start time is during market break: rearrange data with first starting point 
          as market start point (if output data length is not zero)
        - If market break is within start and end times: rearrange timestamps of data 
          at 22:01-> 22:59, adjust behind data by corresponding time difference
        """      
        # Get the last data item's timestamp
        last_item = transformed_data[-1]
        last_timestamp_str = last_item['time']
        last_timestamp_dt = datetime.fromisoformat(last_timestamp_str.replace('Z', '+00:00'))
        last_timestamp_int = int(last_timestamp_dt.timestamp())
        # Check if end_time_int doesn't exactly match last data item timestamp
        if end_time_int == last_timestamp_int:
            return transformed_data
        if last_timestamp_int == PriceDataProvider._get_xau_market_end_point(last_timestamp_dt).timestamp():
            return transformed_data
        print("[FIX] Hey, Gotcha!, I'm fixing the distorted data...")  
        
        start_dt = datetime.fromtimestamp(start_time_int, tz=timezone.utc)
        
        # Case 1: If start time is during a market break
        if PriceDataProvider._is_xau_market_break(start_dt):            
            # Calculate time difference from market start to first data point
            first_item = transformed_data[0]
            first_timestamp_str = first_item['time']
            first_timestamp_dt = datetime.fromisoformat(first_timestamp_str.replace('Z', '+00:00'))
            first_timestamp_int = int(first_timestamp_dt.timestamp())
            market_start = PriceDataProvider._get_xau_market_start_point(start_dt)
            market_start_int = int(market_start.timestamp())
            
            time_diff = market_start_int - first_timestamp_int
            
            # Rearrange all data points
            rearranged_data = []
            for item in transformed_data:
                item_timestamp_str = item['time']
                item_timestamp_dt = datetime.fromisoformat(item_timestamp_str.replace('Z', '+00:00'))
                item_timestamp_int = int(item_timestamp_dt.timestamp())
                
                # Adjust timestamp relative to market start
                new_timestamp_int = item_timestamp_int + time_diff
                new_timestamp_dt = datetime.fromtimestamp(new_timestamp_int, tz=timezone.utc)
                
                rearranged_data.append({
                    'time': new_timestamp_dt.isoformat(),
                    'price': item['price']
                })
            
            return rearranged_data
        
        # Case 2: If market break is within start and end times
        has_market_break, market_break_timestamp = PriceDataProvider._has_market_break_between(transformed_data)
        if has_market_break:
            market_break_timestamp_dt = datetime.fromisoformat(market_break_timestamp.replace('Z', '+00:00'))
            market_break_timestamp_int = int(market_break_timestamp_dt.timestamp())
            # Find data points that fall within market break (22:01-22:58)
            # Map them to 22:01->22:59, 23:01->23:59, ... and rearrange all subsequent data items
            rearranged_data = []
            for item in transformed_data:
                item_timestamp_str = item['time']
                item_timestamp_dt = datetime.fromisoformat(item_timestamp_str.replace('Z', '+00:00'))
                item_timestamp_int = int(item_timestamp_dt.timestamp())
                if item_timestamp_int >= market_break_timestamp_int:
                    time_diff = 58 * 60
                    new_timestamp_int = item_timestamp_int + time_diff
                    new_timestamp_dt = datetime.fromtimestamp(new_timestamp_int, tz=timezone.utc)
                    rearranged_data.append({
                        'time': new_timestamp_dt.isoformat(),
                        'price': item['price']
                    })
                else:
                    rearranged_data.append(item)
            
            return rearranged_data
        print("++++++ Hmm? Another matter exists? The following result seems like distorted data, Becareful with this results!")
        return transformed_data
