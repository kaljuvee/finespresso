import logging
import time
from utils.db.news_db_util import Session, News, update_news_tickers
from utils.db.instrument_db_util import get_instrument_by_company_name
from sqlalchemy import select, or_

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the list of publishers
#PUBLISHERS = ['omx', 'baltics', 'euronext', 'globenewswire_biotech', 'globenewswire_healthcare', 'globenewswire_financialservices', 'globenewswire_technology', 'globenewswire_realestate', 'globenewswire_consumerproducts', 'globenewswire_energy', 'globenewswire_industrials', 'globenewswire_telecommunications', 'globenewswire_utilities', 'globenewswire_materials', 'globenewswire_financialservices', 'globenewswire_technology', 'globenewswire_realestate', 'globenewswire_consumerproducts', 'globenewswire_energy', 'globenewswire_industrials', 'globenewswire_telecommunications', 'globenewswire_utilities', 'globenewswire_materials']

PUBLISHERS = ['globenewswire_biotech'] 
# globenewswire_biotech
def get_news_to_update(publisher):
    logging.info(f"Retrieving news items to update for publisher: {publisher}")
    session = Session()
    try:
        query = select(News).where(
            News.publisher == publisher,
            or_(
                News.yf_ticker.is_(None),
                News.yf_ticker == 'N/A',
                News.ticker.is_(None),
                News.ticker_url.is_(None),
                News.instrument_id.is_(None)
            )
        )
        result = session.execute(query)
        news_items = result.scalars().all()
        count = len(news_items)
        logging.info(f"Retrieved {count} news items to update for {publisher}")
        return news_items
    finally:
        session.close()

def process_news_items(news_items):
    news_items_with_data = []
    total_items = len(news_items)
    
    for index, item in enumerate(news_items, 1):
        instrument = get_instrument_by_company_name(item.company)
        if instrument:
            ticker = instrument.ticker if instrument.ticker and (not item.ticker or item.ticker == 'N/A') else None
            yf_ticker = instrument.yf_ticker if instrument.yf_ticker and (not item.yf_ticker or item.yf_ticker == 'N/A') else None
            instrument_id = instrument.id if not item.instrument_id else None
            ticker_url = instrument.url if instrument.url and not item.ticker_url else None
            
            if any([ticker, yf_ticker, instrument_id, ticker_url]):
                news_items_with_data.append((item.id, ticker, yf_ticker, instrument_id, ticker_url))
        
        if index % 100 == 0 or index == total_items:
            logging.info(f"Processed {index}/{total_items} items")
    
    if news_items_with_data:
        update_news_tickers(news_items_with_data)
    
    updated_count = len(news_items_with_data)
    skipped_count = total_items - updated_count
    logging.info(f"Prepared {updated_count} news items for update")
    logging.info(f"Skipped {skipped_count} news items (no matching instrument or no fields to update)")
    
    return updated_count, skipped_count

def process_publisher(publisher):
    logging.info(f"Processing publisher: {publisher}")
    
    news_to_update = get_news_to_update(publisher)
    if not news_to_update:
        logging.info(f"No news items to update found for {publisher}. Skipping.")
        return 0, 0
    
    return process_news_items(news_to_update)

def main():
    start_time = time.time()
    logging.info("Starting ticker enrichment task")
    
    total_updated = 0
    total_skipped = 0
    for publisher in PUBLISHERS:
        updated, skipped = process_publisher(publisher)
        total_updated += updated
        total_skipped += skipped
    
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Ticker enrichment task completed for all publishers.")
    logging.info(f"Total updated: {total_updated}")
    logging.info(f"Total skipped: {total_skipped}")
    logging.info(f"Duration: {duration:.2f} seconds")

if __name__ == "__main__":
    main()
