import pandas as pd
import time
from datetime import timedelta
import datetime
import holidays
from datetime import datetime
import date_util
import numpy as np
import yfinance as yf

index_symbol = 'SPY'

def set_prices(row):
    symbol = row['ticker']

    # Determine the 'today' date based on the 'published' column
    if isinstance(row['published_est'], pd.Timestamp):
        today_date = row['published_est'].to_pydatetime()
    else:
        today_date = datetime.strptime(row['published_est'], '%Y-%m-%d %H:%M:%S%z')

    # Calculate yesterday and tomorrow dates
    yesterday_date, tomorrow_date = date_util.adjust_dates_for_weekends(today_date)
    
        # Convert dates to the yfinance format
    yf_today_date = today_date.strftime('%Y-%m-%d')
    yf_yesterday_date = yesterday_date.strftime('%Y-%m-%d')
    yf_tomorrow_date = tomorrow_date.strftime('%Y-%m-%d')

    try:
        # Fetch stock data for 3 consecutive days
        data = yf.download(symbol, interval='1d', start=yf_yesterday_date, end=yf_tomorrow_date)
        index_data = yf.download(index_symbol, interval='1d', start=yf_yesterday_date, end=yf_tomorrow_date)
        
        # Determine prices based on the 'market' column value
        if row['market'] == 'market_open':
            row['begin_price'] = data.loc[yf_today_date]['Open']
            row['end_price'] = data.loc[yf_today_date]['Close']
            row['index_begin_price'] = index_data.loc[yf_today_date]['Open']
            row['index_end_price'] = index_data.loc[yf_today_date]['Close']
        elif row['market'] == 'pre_market':
            row['begin_price'] = data.loc[yf_yesterday_date]['Close']
            row['end_price'] = data.loc[yf_today_date]['Open']
            row['index_begin_price'] = index_data.loc[yf_yesterday_date]['Close']
            row['index_end_price'] = index_data.loc[yf_today_date]['Open']
        elif row['market'] == 'after_market':
            row['begin_price'] = data.loc[yf_today_date]['Close']
            row['end_price'] = data.loc[yf_tomorrow_date]['Open']
            row['index_begin_price'] = index_data.loc[yf_today_date]['Close']
            row['index_end_price'] = index_data.loc[yf_tomorrow_date]['Open']
    except Exception as e:
        print(f"Error processing {row['ticker']}: {e}")
        # Handle the error, e.g., by setting prices to None or a default value
        row['begin_price'] = None
        row['end_price'] = None
        row['index_begin_price'] = None
        row['index_end_price'] = None
    return row

def create_price_moves(news_df):
    news_df = news_df.reset_index(drop=True)
    processed_rows = []  # List to store processed rows

    for index, row in news_df.iterrows():
        try:
            # Process each row with set_prices
            processed_row = set_prices(row)
            processed_rows.append(processed_row)
        except Exception as e:
            # Skip the problematic row or handle the error in some way
            print(f"Skipping row {index} due to error: {e}")
            continue

    # Convert the list of processed rows back into a DataFrame
    processed_df = pd.DataFrame(processed_rows)

    # Check if the required columns exist before filtering
    required_price_columns = ['begin_price', 'end_price', 'index_begin_price', 'index_end_price']
    missing_columns = [col for col in required_price_columns if col not in processed_df.columns]
    if missing_columns:
        print(f"Warning: Missing columns in the DataFrame: {missing_columns}")
        return processed_df  # Return the processed DataFrame as is if critical columns are missing

    # Ensure that all required columns have no NaN values
    processed_df.dropna(subset=required_price_columns, inplace=True)

    # Calculate returns and other metrics only if all required data is available
    try:
        processed_df['daily_return'] = (processed_df['end_price'] - processed_df['begin_price']) / processed_df['begin_price']
        processed_df['index_return'] = (processed_df['index_end_price'] - processed_df['index_begin_price']) / processed_df['index_begin_price']
        processed_df['daily_alpha'] = processed_df['daily_return'] - processed_df['index_return']
        processed_df['actual_action'] = np.where(processed_df['daily_alpha'] >= 0, 'long', 'short')
    except Exception as e:
        print(f"Error in calculating returns: {e}")

    return processed_df

