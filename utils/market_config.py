import pytz
from datetime import time
import pandas as pd

# Market configuration
MARKET_TZ = pytz.timezone('America/New_York')  # Eastern Time
MARKET_OPEN = pd.Timestamp('09:30').time()
MARKET_CLOSE = pd.Timestamp('16:00').time()

def localize_time(dt):
    """Localize naive datetime to market timezone or convert from other timezone"""
    if dt.tzinfo is None:
        return MARKET_TZ.localize(dt)
    return dt.astimezone(MARKET_TZ) 