import pandas as pd
import yfinance as yf
import logging
import os
from datetime import datetime, time, timedelta
import numpy as np
from utils.date.date_adjuster import get_previous_trading_day, get_next_trading_day
import sys

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/info.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

index_symbol = 'SPY'
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

def get_trade_time(published_date, market):
    """
    Determine the trade entry time based on published date and market period
    """
    if isinstance(published_date, str):
        published_date = datetime.strptime(published_date, '%Y-%m-%d %H:%M:%S%z')
    
    if market == 'regular_market':
        # Five minutes after publication
        return published_date + timedelta(minutes=5)
    elif market == 'pre_market':
        # Market open (9:30) same day
        return datetime.combine(published_date.date(), MARKET_OPEN, published_date.tzinfo)
    elif market == 'after_market':
        # Market open (9:30) next trading day
        next_trading_day = get_next_trading_day(published_date.date())
        return datetime.combine(next_trading_day, MARKET_OPEN, published_date.tzinfo)
    
    return None

def determine_market_period(published_date):
    """
    Determine if the publication time falls in pre-market, regular market, or after-market
    """
    if isinstance(published_date, str):
        published_date = datetime.strptime(published_date, '%Y-%m-%d %H:%M:%S%z')
    
    pub_time = published_date.time()
    
    if MARKET_OPEN <= pub_time < MARKET_CLOSE:
        return 'regular_market'
    elif MARKET_CLOSE <= pub_time:
        return 'after_market'
    else:  # time(0, 0) <= pub_time < MARKET_OPEN
        return 'pre_market'

def get_intraday_prices(symbol, date, interval='1m'):
    """
    Get intraday price data for a specific date
    """
    try:
        # Check if date is within last 30 days (Yahoo's limitation)
        current_date = datetime.now().date()
        days_difference = (current_date - date).days
        
        if days_difference > 25:  # Using 25 days as safe limit
            logger.warning(f"Date {date} is more than 25 days old. Intraday data not available for {symbol}")
            return pd.DataFrame()
            
        # Download intraday data
        start_date = date.strftime('%Y-%m-%d')
        end_date = (date + timedelta(days=1)).strftime('%Y-%m-%d')
        data = yf.download(symbol, start=start_date, end=end_date, interval=interval, progress=False)
        
        if data.empty:
            logger.warning(f"No intraday data available for {symbol} on {date}")
        
        return data
    except Exception as e:
        logger.error(f"Error fetching intraday data for {symbol}: {e}")
        return pd.DataFrame()

def set_prices(row):
    # Convert the input row to a copy to avoid SettingWithCopyWarning
    row = row.copy()
    
    symbol = row['yf_ticker']
    logger.info(f"Processing price data for {symbol}")

    if not symbol:  # Skip if no ticker symbol
        logger.warning(f"No ticker symbol found for news_id {row.get('news_id')}")
        return row

    if isinstance(row['published_date'], pd.Timestamp):
        today_date = row['published_date'].to_pydatetime()
    else:
        today_date = datetime.strptime(row['published_date'], '%Y-%m-%d %H:%M:%S%z')
    
    # Determine market based on publication time
    market = determine_market_period(today_date)
    row['market'] = market
    
    # Set trade time
    row['trade_time'] = get_trade_time(today_date, market)
    
    # Find the previous and next trading days
    today_date_only = today_date.date()
    previous_trading_day = get_previous_trading_day(today_date_only)
    next_trading_day = get_next_trading_day(today_date_only)
    
    yf_previous_date = previous_trading_day.strftime('%Y-%m-%d')
    yf_today_date = today_date_only.strftime('%Y-%m-%d')
    yf_next_date = next_trading_day.strftime('%Y-%m-%d')

    try:
        # Download data
        data = yf.download(symbol, yf_previous_date, yf_next_date, progress=False)
        index_data = yf.download(index_symbol, yf_previous_date, yf_next_date, progress=False)
        
        if data.empty or index_data.empty:
            logger.warning(f"No data available for {symbol} or {index_symbol}")
            return row

        # Safely get prices using get() method with a default value
        try:
            if market == 'regular_market':
                # Check if date is within last 25 days
                current_date = datetime.now().date()
                days_difference = (current_date - today_date_only).days
                
                if days_difference <= 25:
                    # Get intraday data for entry
                    intraday_data = get_intraday_prices(symbol, today_date_only)
                    
                    if not intraday_data.empty:
                        # Get entry time (5 minutes after publication)
                        entry_time = get_trade_time(today_date, market)
                        
                        # Find the closest price data after entry_time
                        valid_times = intraday_data.index[intraday_data.index >= entry_time]
                        if len(valid_times) > 0:
                            entry_idx = valid_times[0]
                            row['begin_price'] = float(intraday_data.loc[entry_idx]['Open'])
                            row['end_price'] = float(data.loc[yf_today_date]['Close'].iloc[0])
                            row['index_begin_price'] = float(index_data.loc[yf_today_date]['Open'].iloc[0])
                            row['index_end_price'] = float(index_data.loc[yf_today_date]['Close'].iloc[0])
                        else:
                            logger.warning(f"No valid intraday data found after entry time for {symbol}")
                            return row
                    else:
                        logger.warning(f"Falling back to daily data for {symbol} due to missing intraday data")
                        # Fallback to daily data
                        row['begin_price'] = float(data.loc[yf_today_date]['Open'].iloc[0])
                        row['end_price'] = float(data.loc[yf_today_date]['Close'].iloc[0])
                        row['index_begin_price'] = float(index_data.loc[yf_today_date]['Open'].iloc[0])
                        row['index_end_price'] = float(index_data.loc[yf_today_date]['Close'].iloc[0])
                else:
                    logger.info(f"Date {today_date_only} is more than 25 days old, using daily data for {symbol}")
                    # Use daily data for older dates
                    row['begin_price'] = float(data.loc[yf_today_date]['Open'].iloc[0])
                    row['end_price'] = float(data.loc[yf_today_date]['Close'].iloc[0])
                    row['index_begin_price'] = float(index_data.loc[yf_today_date]['Open'].iloc[0])
                    row['index_end_price'] = float(index_data.loc[yf_today_date]['Close'].iloc[0])
            elif market == 'pre_market':
                row['begin_price'] = float(data.loc[yf_previous_date]['Close'].iloc[0])
                row['end_price'] = float(data.loc[yf_today_date]['Open'].iloc[0])
                row['index_begin_price'] = float(index_data.loc[yf_previous_date]['Close'].iloc[0])
                row['index_end_price'] = float(index_data.loc[yf_today_date]['Open'].iloc[0])
            elif market == 'after_market':
                if yf_next_date in data.index and yf_next_date in index_data.index:
                    row['begin_price'] = float(data.loc[yf_today_date]['Close'].iloc[0])
                    row['end_price'] = float(data.loc[yf_next_date]['Open'].iloc[0])
                    row['index_begin_price'] = float(index_data.loc[yf_today_date]['Close'].iloc[0])
                    row['index_end_price'] = float(index_data.loc[yf_next_date]['Open'].iloc[0])
                else:
                    logger.warning(f"Next day data not available for {symbol}")
                    return row

            # Get volume data
            row['Volume'] = float(data.loc[yf_today_date]['Volume'].iloc[0])

        except KeyError as e:
            logger.warning(f"Missing data for {symbol} on date {e}")
            return row
        except Exception as e:
            logger.error(f"Error processing prices for {symbol}: {str(e)}")
            return row

        # Calculate price changes only if we have all required prices
        if all(row.get(key) is not None for key in ['begin_price', 'end_price', 'index_begin_price', 'index_end_price']):
            row['price_change'] = row['end_price'] - row['begin_price']
            row['index_price_change'] = row['index_end_price'] - row['index_begin_price']
            row['price_change_percentage'] = (row['price_change'] / row['begin_price']) * 100
            row['index_price_change_percentage'] = (row['index_price_change'] / row['index_begin_price']) * 100
            row['daily_alpha'] = row['price_change_percentage'] - row['index_price_change_percentage']
            row['actual_side'] = 'UP' if row['price_change_percentage'] >= 0 else 'DOWN'

    except Exception as e:
        logger.error(f"Error processing {symbol}: {str(e)}")
        logger.exception("Detailed traceback:")
        
    return row

def create_price_moves(news_df):
    logger.info(f"Starting to create price moves for {len(news_df)} news items")
    news_df = news_df.reset_index(drop=True)
    processed_rows = []

    for index, row in news_df.iterrows():
        try:
            logger.info(f"Processing row {index} for ticker {row['yf_ticker']}")
            processed_row = set_prices(row)
            processed_rows.append(processed_row)
        except Exception as e:
            logger.error(f"Error processing row {index} for ticker {row['yf_ticker']}: {e}")
            logger.exception("Detailed traceback:")
            continue

    processed_df = pd.DataFrame(processed_rows)
    logger.info(f"Processed {len(processed_df)} rows successfully")

    required_price_columns = ['begin_price', 'end_price', 'index_begin_price', 'index_end_price']
    missing_columns = [col for col in required_price_columns if col not in processed_df.columns]
    if missing_columns:
        logger.warning(f"Missing columns in the DataFrame: {missing_columns}")
        return processed_df

    original_len = len(processed_df)
    processed_df.dropna(subset=required_price_columns, inplace=True)
    logger.info(f"Removed {original_len - len(processed_df)} rows with NaN values")

    return processed_df 