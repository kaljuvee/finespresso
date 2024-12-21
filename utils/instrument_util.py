import pandas as pd
from utils.db.instrument_db_util import get_instrument_by_company_name
from utils.logging.log_util import get_logger

logger = get_logger(__name__)

def get_ticker(df):
    """
    Get tickers for a dataframe of news items.
    
    Args:
    df (pd.DataFrame): Dataframe containing news items with 'company' column.
    
    Returns:
    pd.DataFrame: Updated dataframe with 'ticker', 'yf_ticker', 'instrument_id', and 'ticker_url' columns.
    """
    logger.info(f"Getting tickers for {len(df)} news items")
    
    for index, row in df.iterrows():
        instrument = get_instrument_by_company_name(row['company'])
        if instrument:
            df.at[index, 'ticker'] = instrument.ticker if instrument.ticker and (not row.get('ticker') or row.get('ticker') == 'N/A') else row.get('ticker')
            df.at[index, 'yf_ticker'] = instrument.yf_ticker if instrument.yf_ticker and (not row.get('yf_ticker') or row.get('yf_ticker') == 'N/A') else row.get('yf_ticker')
            df.at[index, 'instrument_id'] = instrument.id if not row.get('instrument_id') else row.get('instrument_id')
            df.at[index, 'ticker_url'] = instrument.url if instrument.url and not row.get('ticker_url') else row.get('ticker_url')
    
    logger.info(f"Finished getting tickers for {len(df)} news items")
    return df

def main():
    # Sample news dataframe for testing
    sample_news = pd.DataFrame([
        {
            'title': 'Company A Announces Q2 Results',
            'link': 'http://example.com/news1',
            'company': 'BILENDI',
            'published_date': '2023-07-15 10:00:00',
            'content': 'Company A announced their Q2 results today...',
            'publisher': 'Example News',
            'status': 'raw'
        },
        {
            'news_id': 2,
            'title': 'Company B Launches New Product',
            'link': 'http://example.com/news2',
            'company': 'Intapp',
            'published_date': '2023-07-16 14:30:00',
            'content': 'Company B has launched a groundbreaking new product...',
            'publisher': 'Example News',
            'status': 'raw'
        }
    ])
    
    logger.info("Starting ticker lookup process")
    updated_df = get_ticker(sample_news)
    
    logger.info("Updated dataframe:")
    logger.info(updated_df.to_string())

if __name__ == "__main__":
    main()

