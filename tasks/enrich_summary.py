import logging
from sqlalchemy import select
from utils.news_db_util import Session, News
from utils.enrich_util import enrich_summary_from_url
import pandas as pd
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the list of publishers
PUBLISHERS = ['omx', 'baltics', 'euronext', 'globenewswire_biotech']

def get_news_without_summary(publisher):
    logging.info(f"Retrieving news items without reasons for publisher: {publisher}")
    session = Session()
    try:
        query = select(News).where(
            (News.reason.is_(None) | (News.reason == '')), 
            News.publisher == publisher,
            News.status == 'tagged'
        )
        result = session.execute(query)
        news_items = result.scalars().all()
        count = len(news_items)
        print(f"Retrieved {count} news items without reasons for {publisher}")
        logging.info(f"Retrieved {count} news items without reasons for {publisher}")
        return news_items
    finally:
        session.close()

def news_to_dataframe(news_items):
    logging.info("Converting news items to DataFrame")
    df = pd.DataFrame([{'id': item.id, 'link': item.link} for item in news_items])
    logging.info(f"Created DataFrame with {len(df)} rows")
    return df

def update_summaries(enriched_df):
    logging.info("Updating database with enriched reasons")
    session = Session()
    try:
        updated_count = 0
        total_items = len(enriched_df)
        for index, row in enriched_df.iterrows():
            news_item = session.get(News, row['id'])
            if news_item and 'reason' in row:
                news_item.reason = row['reason']
                news_item.status = 'reasoned'  # Update status to 'reasoned'
                updated_count += 1
            
            if (index + 1) % 10 == 0 or index == total_items - 1:
                logging.info(f"Updated {index + 1}/{total_items} items")
        
        session.commit()
        logging.info(f"Successfully updated {updated_count} news items with reasons and set status to 'reasoned'")
    except Exception as e:
        logging.error(f"Error updating reasons: {str(e)}")
        session.rollback()
    finally:
        session.close()

def process_publisher(publisher):
    logging.info(f"Processing publisher: {publisher}")
    
    news_without_summary = get_news_without_summary(publisher)
    if not news_without_summary:
        logging.info(f"No news items without reasons found for {publisher}. Skipping.")
        return
    
    news_df = news_to_dataframe(news_without_summary)
    
    logging.info(f"Enriching news items with reasons for {publisher}")
    enriched_df = enrich_summary_from_url(news_df)
    
    update_summaries(enriched_df)

def main():
    start_time = time.time()
    logging.info("Starting summary enrichment task")
    
    for publisher in PUBLISHERS:
        process_publisher(publisher)
    
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Summary enrichment task completed for all publishers. Duration: {duration:.2f} seconds")

if __name__ == "__main__":
    main()
