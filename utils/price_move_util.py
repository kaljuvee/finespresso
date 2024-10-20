import pandas as pd
import yfinance as yf
import logging
import os
from datetime import datetime, time
import numpy as np
from utils.db.price_move_db_util import store_price_move, PriceMove
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
EXCHANGE = 'NASDAQ'

def get_price_data(ticker, published_date):
    logger.info(f"Getting price data for {ticker} on {published_date}")
    row = pd.Series({'ticker': ticker, 'published_date': published_date, 'market': 'market_open'})  # Assume market_open, adjust if needed
    processed_row = set_prices(row)
    
    if processed_row['begin_price'] is None or processed_row['end_price'] is None:
        logger.warning(f"Unable to get price data for {ticker} on {published_date}")
        return None

def set_prices(row):
    # Convert the input row to a copy to avoid SettingWithCopyWarning
    row = row.copy()
    
    symbol = row['yf_ticker']
    logger.info(f"Processing price data for {symbol}")

    if isinstance(row['published_date'], pd.Timestamp):
        today_date = row['published_date'].to_pydatetime()
    else:
        today_date = datetime.strptime(row['published_date'], '%Y-%m-%d %H:%M:%S%z')
    logger.info(f"Today's date for {symbol}: {today_date}")

    # Determine market based on publication time
    pub_time = today_date.time()
    if time(9, 30) <= pub_time < time(16, 0):
        market = 'regular_market'
    elif time(16, 0) <= pub_time:
        market = 'after_market'
    elif time(0, 0) <= pub_time < time(9, 30):
        market = 'pre_market'
    logger.info(f"Determined market for {symbol}: {market}")

    # Find the previous and next trading days
    today_date_only = today_date.date()
    previous_trading_day = get_previous_trading_day(today_date_only)
    next_trading_day = get_next_trading_day(today_date_only)

    logger.info(f"Adjusted dates for {symbol}: Previous: {previous_trading_day}, Today: {today_date_only}, Next: {next_trading_day}")
    
    yf_previous_date = previous_trading_day.strftime('%Y-%m-%d')
    yf_today_date = today_date_only.strftime('%Y-%m-%d')
    yf_next_date = next_trading_day.strftime('%Y-%m-%d')

    try:
        logger.info(f"Fetching stock data for {symbol}")
        data = yf.download(symbol, yf_previous_date, yf_next_date)
        logger.info(f"Fetched data for {symbol}:\n{data}")
        
        logger.info(f"Fetching index data for {index_symbol}")
        index_data = yf.download(index_symbol, yf_previous_date, yf_next_date)
        logger.info(f"Fetched index data:\n{index_data}")
        
        if data.empty or index_data.empty:
            logger.warning(f"No data available for {symbol} or index in the specified date range")
            raise ValueError("No data available for the specified date range")

        logger.info(f"Determining prices for {symbol} based on market: {market}")

        if market == 'pre_market':
            row['begin_price'] = data.loc[yf_previous_date, 'Close']
            row['end_price'] = data.loc[yf_today_date, 'Open']
            row['index_begin_price'] = index_data.loc[yf_previous_date, 'Close']
            row['index_end_price'] = index_data.loc[yf_today_date, 'Open']
            logger.info(f"Pre-market prices for {symbol}: Begin: {row['begin_price']}, End: {row['end_price']}")
            logger.info(f"Pre-market index prices: Begin: {row['index_begin_price']}, End: {row['index_end_price']}")

        elif market == 'regular_market':
            row['begin_price'] = data.loc[yf_today_date, 'Open']
            row['end_price'] = data.loc[yf_today_date, 'Close']
            row['index_begin_price'] = index_data.loc[yf_today_date, 'Open']
            row['index_end_price'] = index_data.loc[yf_today_date, 'Close']
        elif market == 'after_market':
            row['begin_price'] = data.loc[yf_today_date, 'Close']
            row['end_price'] = data.loc[yf_next_date, 'Open']
            row['index_begin_price'] = index_data.loc[yf_today_date, 'Close']
            row['index_end_price'] = index_data.loc[yf_next_date, 'Open']
        else:
            logger.warning(f"Unknown market type: {market}. Defaulting to regular_market.")
            row['begin_price'] = data.loc[yf_today_date, 'Open']
            row['end_price'] = data.loc[yf_today_date, 'Close']
            row['index_begin_price'] = index_data.loc[yf_today_date, 'Open']
            row['index_end_price'] = index_data.loc[yf_today_date, 'Close']

        # Calculate derived values
        row['price_change'] = row['end_price'] - row['begin_price']
        row['index_price_change'] = row['index_end_price'] - row['index_begin_price']
        row['price_change_percentage'] = (row['price_change'] / row['begin_price']) * 100  # Multiply by 100
        row['index_price_change_percentage'] = (row['index_price_change'] / row['index_begin_price']) * 100  # Multiply by 100
        row['daily_alpha'] = row['price_change_percentage'] - row['index_price_change_percentage']
        row['actual_side'] = 'UP' if row['price_change_percentage'] >= 0 else 'DOWN'
        row['Volume'] = data.loc[yf_today_date, 'Volume']
        row['market'] = market
        logger.info(f"Successfully set prices for {symbol}")
    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")
        logger.exception("Detailed traceback:")
        # Set all relevant fields to None in case of an error
        for field in ['begin_price', 'end_price', 'index_begin_price', 'index_end_price', 
                      'price_change', 'index_price_change', 'price_change_percentage', 
                      'index_price_change_percentage', 'daily_alpha', 'actual_side', 'Volume']:
            row[field] = None
        row['market'] = market

    logger.info(f"Processed row for {symbol}: {row}")
    return row

def create_price_moves(news_df):
    logger.info(f"Starting to create price moves for {len(news_df)} news items")
    news_df = news_df.reset_index(drop=True)
    processed_rows = []

    for index, row in news_df.iterrows():
        try:
            logger.info(f"Processing row {index} for ticker {row['yf_ticker']}")  # Changed from 'ticker' to 'yf_ticker'
            processed_row = set_prices(row)
            processed_rows.append(processed_row)
        except Exception as e:
            logger.error(f"Error processing row {index} for ticker {row['yf_ticker']}: {e}")  # Changed from 'ticker' to 'yf_ticker'
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

    try:
        logger.info("Calculating alpha and setting actual side")
        processed_df['daily_alpha'] = processed_df['price_change_percentage'] - processed_df['index_price_change_percentage']
        processed_df['actual_side'] = np.where(processed_df['price_change_percentage'] >= 0, 'UP', 'DOWN')
        logger.info("Successfully calculated alpha and set actual side")
    except Exception as e:
        logger.error(f"Error in calculations: {e}")

    logger.info(f"Finished creating price moves. Final DataFrame has {len(processed_df)} rows")
    
    # Store price moves in the database
    for _, row in processed_df.iterrows():
        try:
            price_move = create_price_move(
                news_id=row['news_id'],
                ticker=row['yf_ticker'],  # Changed from 'ticker' to 'yf_ticker'
                published_date=row['published_date'],
                begin_price=row['begin_price'],
                end_price=row['end_price'],
                index_begin_price=row['index_begin_price'],
                index_end_price=row['index_end_price'],
                volume=row.get('Volume'),
                market=row['market'],
                price_change=row['price_change'],
                price_change_percentage=row['price_change_percentage'],
                index_price_change=row['index_price_change'],
                index_price_change_percentage=row['index_price_change_percentage'],
                actual_side=row['actual_side']
            )
            store_price_move(price_move)
        except Exception as e:
            logger.error(f"Error storing price move for news_id {row['news_id']}: {e}")

    return processed_df

def create_price_move(news_id, ticker, published_date, begin_price, end_price, index_begin_price, index_end_price, volume, market, price_change, price_change_percentage, index_price_change, index_price_change_percentage, actual_side, predicted_side=None):
    daily_alpha = price_change_percentage - index_price_change_percentage
    return PriceMove(
        news_id=news_id,
        ticker=ticker,
        published_date=published_date,
        begin_price=begin_price,
        end_price=end_price,
        index_begin_price=index_begin_price,
        index_end_price=index_end_price,
        volume=volume,
        market=market,  # This is correct
        price_change=price_change,
        price_change_percentage=price_change_percentage,
        index_price_change=index_price_change,
        index_price_change_percentage=index_price_change_percentage,
        daily_alpha=daily_alpha,
        actual_side=actual_side,
        predicted_side=predicted_side
    )
