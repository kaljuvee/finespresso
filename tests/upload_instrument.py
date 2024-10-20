import pandas as pd
import logging
from utils.db.instrument_db_util import save_instrument

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_csv_to_df(file_path):
    logging.info(f"Reading CSV file: {file_path}")
    return pd.read_csv(file_path)

def filter_distinct_issuers(df):
    logging.info("Filtering distinct issuers")
    return df.drop_duplicates(subset=['issuer', 'ticker'], keep='first')

def main():
    file_path = 'data/instrument_nasdaq_biotech.csv'
    df = read_csv_to_df(file_path)
    
    logging.info(f"Original dataframe shape: {df.shape}")
    
    distinct_df = filter_distinct_issuers(df)
    logging.info(f"Filtered dataframe shape: {distinct_df.shape}")
    
    save_instrument(distinct_df)
    logging.info("Instruments uploaded successfully.")

if __name__ == "__main__":
    main()
