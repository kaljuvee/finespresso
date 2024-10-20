import logging
from sqlalchemy import select, func
from utils.db.news_db_util import Session, News
from utils.enrich_util import enrich_content_from_url
import pandas as pd
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the list of publishers
PUBLISHERS = ['omx', 'baltics', 'euronext','globenewswire_biotech']

def get_news_without_content_count(publisher):
    session = Session()
    try:
        count = session.query(func.count(News.id)).filter(
            (News.content.is_(None) | (News.content == '')),
            News.publisher == publisher
        ).scalar()
        return count
    finally:
        session.close()

def get_news_without_content(publisher, limit=None):
    logging.info(f"Retrieving news items without content for publisher: {publisher}")
    session = Session()
    try:
        # Change this query to correctly identify null content
        query = select(News).where(
            (News.content.is_(None) | (News.content == '')),
            News.publisher == publisher
        )
        if limit:
            query = query.limit(limit)
        result = session.execute(query)
        news_items = result.scalars().all()
        count = len(news_items)
        logging.info(f"Retrieved {count} news items without content for {publisher}")
        return news_items
    finally:
        session.close()

def news_to_dataframe(news_items):
    logging.info("Converting news items to DataFrame")
    df = pd.DataFrame([
        {
            'id': item.id,
            'title': item.title,
            'link': item.link,
            'status': item.status
        } for item in news_items
    ])
    logging.info(f"Created DataFrame with {len(df)} rows")
    return df

def update_enriched_news(enriched_df):
    logging.info(f"Updating database with enriched content for {len(enriched_df)} items")
    session = Session()
    try:
        updated_count = 0
        total_items = len(enriched_df)
        for index, row in enriched_df.iterrows():
            news_item = session.get(News, row['id'])
            if news_item:
                news_item.content = row['content']
                if news_item.status != 'fully_enriched':
                    news_item.status = 'content_enriched'
                updated_count += 1
            
            if (index + 1) % 10 == 0 or index == total_items - 1:
                logging.info(f"Updated {index + 1}/{total_items} items")
        
        session.commit()
        logging.info(f"Successfully updated {updated_count} news items with enriched content")
        return updated_count
    except Exception as e:
        logging.error(f"Error updating enriched news: {str(e)}")
        session.rollback()
        return 0
    finally:
        session.close()

def process_publisher(publisher, batch_size=500):
    logging.info(f"Processing publisher: {publisher}")
    
    total_without_content = get_news_without_content_count(publisher)
    logging.info(f"Total news items without content for {publisher}: {total_without_content}")
    
    news_without_content = get_news_without_content(publisher, limit=batch_size)
    if not news_without_content:
        logging.info(f"No news items without content found for {publisher}. Skipping.")
        return 0, 0
    
    news_df = news_to_dataframe(news_without_content)
    
    logging.info(f"Enriching news items with content for {publisher}")
    enriched_df = enrich_content_from_url(news_df)
    
    updated_count = update_enriched_news(enriched_df)
    
    return len(news_without_content), updated_count

def main():
    start_time = time.time()
    logging.info("Starting content enrichment task")
    
    total_processed = 0
    total_updated = 0
    batch_size = 500  # You can adjust this value as needed
    
    for publisher in PUBLISHERS:
        processed, updated = process_publisher(publisher, batch_size)
        total_processed += processed
        total_updated += updated
    
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Content enrichment task completed for all publishers.")
    logging.info(f"Total items processed: {total_processed}")
    logging.info(f"Total items updated: {total_updated}")
    logging.info(f"Duration: {duration:.2f} seconds")

if __name__ == "__main__":
    main()
