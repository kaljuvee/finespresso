import logging
from sqlalchemy import select
from utils.db_util import Session, News, update_news_status
from utils.enrich_util import enrich_tag_from_url
import pandas as pd
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the list of publishers
PUBLISHERS = ['omx', 'baltics', 'euronext']

def get_news_without_tags(publisher):
    logging.info(f"Retrieving clean news items without AI topics for publisher: {publisher}")
    session = Session()
    try:
        query = select(News).where(
            News.ai_topic.is_(None),
            News.publisher == publisher,
            News.status == 'clean'  # Add this condition
        )
        result = session.execute(query)
        news_items = result.scalars().all()
        count = len(news_items)
        logging.info(f"Retrieved {count} clean news items without AI topics for {publisher}")
        return news_items
    finally:
        session.close()

def news_to_dataframe(news_items):
    logging.info("Converting news items to DataFrame")
    df = pd.DataFrame([{'id': item.id, 'link': item.link} for item in news_items])
    logging.info(f"Created DataFrame with {len(df)} rows")
    return df

def update_tags(enriched_df):
    logging.info("Updating database with enriched tags")
    session = Session()
    try:
        updated_count = 0
        total_items = len(enriched_df)
        for index, row in enriched_df.iterrows():
            news_item = session.get(News, row['id'])
            if news_item and 'ai_topic' in row:
                news_item.ai_topic = row['ai_topic']
                news_item.status = 'tagged'  # Update status to 'tagged'
                updated_count += 1
            
            if (index + 1) % 10 == 0 or index == total_items - 1:
                logging.info(f"Updated {index + 1}/{total_items} items")
        
        session.commit()
        logging.info(f"Successfully updated {updated_count} news items with tags and set status to 'tagged'")
    except Exception as e:
        logging.error(f"Error updating tags: {str(e)}")
        session.rollback()
    finally:
        session.close()

def process_publisher(publisher):
    logging.info(f"Processing publisher: {publisher}")
    
    news_without_tags = get_news_without_tags(publisher)
    if not news_without_tags:
        logging.info(f"No news items without AI topics found for {publisher}. Skipping.")
        return
    
    news_df = news_to_dataframe(news_without_tags)
    
    logging.info(f"Enriching news items with tags for {publisher}")
    enriched_df = enrich_tag_from_url(news_df)
    
    update_tags(enriched_df)

def main():
    start_time = time.time()
    logging.info("Starting tag enrichment task")
    
    for publisher in PUBLISHERS:
        process_publisher(publisher)
    
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Tag enrichment task completed for all publishers. Duration: {duration:.2f} seconds")

if __name__ == "__main__":
    main()
