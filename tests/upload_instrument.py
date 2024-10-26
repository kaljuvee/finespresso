import pandas as pd
import sys
import os

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.db.instrument_db_util import save_instrument, get_all_instruments
from utils.logging.log_util import get_logger

logger = get_logger(__name__)

def read_csv_to_df(file_path):
    logger.info(f"Reading CSV file: {file_path}")
    return pd.read_csv(file_path)

def filter_distinct_issuers(df):
    logger.info("Filtering distinct issuers")
    return df.drop_duplicates(subset=['issuer', 'ticker'], keep='first')

def filter_new_instruments(new_df, existing_df):
    logger.info("Filtering out existing instruments")
    existing_tickers = set(existing_df['ticker'])
    new_instruments = new_df[~new_df['ticker'].isin(existing_tickers)]
    return new_instruments

def main():
    file_path = 'data/instrument_nasdaq_altenergy.csv'
    new_df = read_csv_to_df(file_path)
    
    logger.info(f"Original dataframe shape: {new_df.shape}")
    logger.info(f"Number of non-null yf_tickers: {new_df['yf_ticker'].notna().sum()}")
    
    distinct_df = filter_distinct_issuers(new_df)
    logger.info(f"Filtered dataframe shape: {distinct_df.shape}")
    logger.info(f"Number of non-null yf_tickers after filtering: {distinct_df['yf_ticker'].notna().sum()}")
    
    # Get existing instruments
    existing_df = get_all_instruments()
    logger.info(f"Number of existing instruments: {len(existing_df)}")
    
    # Filter out instruments that already exist
    new_instruments = filter_new_instruments(distinct_df, existing_df)
    logger.info(f"Number of new instruments to upload: {len(new_instruments)}")
    
    if not new_instruments.empty:
        logger.info("Uploading new instruments")
        save_instrument(new_instruments)
        logger.info("New instrument upload process completed.")
    else:
        logger.info("No new instruments to upload.")

if __name__ == "__main__":
    main()
