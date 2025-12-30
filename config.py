"""Configuration constants and parameters for the forecasting system."""

# Time intervals (in seconds)
TIME_INCREMENT_5MIN = 300
TIME_INCREMENT_30MIN = 1800

# EWM parameters
EWM_SPAN = 12
MA_WINDOW = 14

# Simulation parameters
PRESENT_NUM_SIMULATIONS = 50
DEFAULT_FORECAST_LENGTH = 86400
IS_FIRST_RUN = False  # Change to false after once started
EWM_INDEX = -2 # TODO: Change to -1 in production

# Weekday names
WEEKDAY_NAMES = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

