import logging
import time
from utils.news_db_util import Session, News, update_news_tickers
from utils.instrument_db_util import get_instrument_by_company_name
from sqlalchemy import select

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the list of publishers
PUBLISHERS = ['omx', 'baltics', 'euronext']

def get_news_without_tickers(publisher):
    logging.info(f"Retrieving news items without tickers for publisher: {publisher}")
    session = Session()
    try:
        query = select(News).where(News.ticker.is_(None), News.publisher == publisher)
        result = session.execute(query)
        news_items = result.scalars().all()
        count = len(news_items)
        logging.info(f"Retrieved {count} news items without tickers for {publisher}")
        return news_items
    finally:
        session.close()

def process_news_items(news_items):
    news_items_with_tickers = []
    total_items = len(news_items)
    for index, item in enumerate(news_items, 1):
        instrument = get_instrument_by_company_name(item.company)
        if instrument:
            ticker = instrument.ticker
            ticker_url = instrument.url
        else:
            ticker = None
            ticker_url = None
        news_items_with_tickers.append((item.id, ticker, ticker_url))
        if index % 10 == 0 or index == total_items:
            logging.info(f"Processed {index}/{total_items} items")
    return news_items_with_tickers

def update_news_tickers(news_items_with_tickers):
    logging.info("Updating database with extracted tickers and URLs")
    session = Session()
    try:
        updated_count = 0
        total_items = len(news_items_with_tickers)
        for index, (news_id, ticker, ticker_url) in enumerate(news_items_with_tickers, 1):
            news_item = session.get(News, news_id)
            if news_item:
                news_item.ticker = ticker
                news_item.ticker_url = ticker_url
                updated_count += 1
            
            if index % 10 == 0 or index == total_items:
                logging.info(f"Updated {index}/{total_items} items")
        
        session.commit()
        logging.info(f"Successfully updated {updated_count} news items with tickers and URLs")
    except Exception as e:
        logging.error(f"Error updating tickers and URLs: {str(e)}")
        session.rollback()
    finally:
        session.close()

def process_publisher(publisher):
    logging.info(f"Processing publisher: {publisher}")
    
    news_without_tickers = get_news_without_tickers(publisher)
    if not news_without_tickers:
        logging.info(f"No news items without tickers found for {publisher}. Skipping.")
        return
    
    news_items_with_tickers = process_news_items(news_without_tickers)
    update_news_tickers(news_items_with_tickers)

def main():
    start_time = time.time()
    logging.info("Starting ticker enrichment task")
    
    for publisher in PUBLISHERS:
        process_publisher(publisher)
    
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Ticker enrichment task completed for all publishers. Duration: {duration:.2f} seconds")

if __name__ == "__main__":
    main()
