import pandas as pd
import time
from datetime import timedelta
import datetime
import holidays
from datetime import datetime

def adjust_dates_for_weekends_and_holidays(today_date):
    us_holidays = holidays.US(years=today_date.year)
    
    # Calculate initial yesterday and tomorrow dates
    yesterday_date = today_date - timedelta(days=1)
    tomorrow_date = today_date + timedelta(days=1)

    # Adjust for weekends and holidays for 'yesterday'
    while yesterday_date.weekday() >= 5 or yesterday_date in us_holidays:
        yesterday_date -= timedelta(days=1)

    # Adjust for weekends and holidays for 'tomorrow'
    while tomorrow_date.weekday() >= 5 or tomorrow_date in us_holidays:
        tomorrow_date += timedelta(days=1)
    
    return yesterday_date, tomorrow_date


def adjust_dates_for_weekends(today_date):
    """
    Adjusts the provided date to ensure that the calculated 'yesterday' and 'tomorrow' dates 
    do not fall on a weekend.
    
    Args:
    - today_date (datetime.datetime): The reference date.

    Returns:
    - tuple: (yesterday_date, tomorrow_date) where neither date falls on a weekend.
    """
    
    # Calculate yesterday and tomorrow dates
    yesterday_date = today_date - timedelta(days=1)
    tomorrow_date = today_date + timedelta(days=1)

    # Adjust for weekends
    # If today is Monday, set yesterday to the previous Friday
    if today_date.weekday() == 0:
        yesterday_date = today_date - timedelta(days=3)
    # If today is Friday, set tomorrow to the following Monday
    elif today_date.weekday() == 4:
        tomorrow_date = today_date + timedelta(days=3)
    # If today is Saturday, adjust both yesterday and tomorrow
    elif today_date.weekday() == 5:
        yesterday_date = today_date - timedelta(days=2)
        tomorrow_date = today_date + timedelta(days=2)
    # If today is Sunday, adjust both yesterday and tomorrow
    elif today_date.weekday() == 6:
        yesterday_date = today_date - timedelta(days=3)
        tomorrow_date = today_date + timedelta(days=1)
    
    return yesterday_date, tomorrow_date
