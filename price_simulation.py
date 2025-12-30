import requests


import numpy as np

from btc_average_calculation import btc_average_calculation


# Hermes Pyth API documentation: https://hermes.pyth.network/docs/

TOKEN_MAP = {
    "BTC": "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43",
    "ETH": "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace",
    "XAU": "765d2ba906dbc32ca17cc11f5310a89e9ee1f6420508c63861f2f8ba4ee34bb2",
    "SOL": "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
}

pyth_base_url = "https://hermes.pyth.network/v2/updates/price/latest"


def simulate_single_price_path(
    current_price, time_increment, time_length, sigma, 
    direction=1, base_drift=0.0, seed=None
):
    """
    Simulate a single price path with controlled direction.
    """
    rng = np.random.default_rng(seed)
    
    one_hour = 3600
    dt = time_increment / one_hour
    num_steps = int(time_length / time_increment)
    std_dev = sigma * np.sqrt(dt)
    
    # Generate random price changes
    price_change_pcts = rng.normal(0, std_dev, size=num_steps)
    
    # Apply directional bias
    if direction != 0:
        directional_bias = abs(price_change_pcts.mean()) * direction * 0.3
        price_change_pcts = price_change_pcts + directional_bias
    
    # Add base drift
    price_change_pcts = price_change_pcts + base_drift
    
    cumulative_returns = np.cumprod(1 + price_change_pcts)
    cumulative_returns = np.insert(cumulative_returns, 0, 1.0)
    price_path = current_price * cumulative_returns
    
    return price_path

def simulate_crypto_price_paths(
    current_price, time_increment, time_length, num_simulations, sigma, close_time
):
    """
    Simulate multiple crypto asset price paths.
    """
    average_price = btc_average_calculation(current_price, close_time)

    price_paths = []
    for _ in range(num_simulations):
        price_path = simulate_single_price_path(
            current_price, time_increment, time_length, sigma
        )
        price_paths.append(price_path)

    return np.array(price_paths), average_price

