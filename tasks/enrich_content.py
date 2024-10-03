import logging
from sqlalchemy import select
from utils.db_util import Session, News
from utils.enrich_util import enrich_content_from_url
import pandas as pd
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the list of publishers
PUBLISHERS = ['omx', 'baltics', 'euronext']

def get_news_without_content(publisher):
    logging.info(f"Retrieving news items without content for publisher: {publisher}")
    session = Session()
    try:
        query = select(News).where(News.content.is_(None), News.publisher == publisher)
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
    logging.info("Updating database with enriched content")
    session = Session()
    try:
        updated_count = 0
        total_items = len(enriched_df)
        for index, row in enriched_df.iterrows():
            news_item = session.get(News, row['id'])
            if news_item:
                news_item.content = row['content']
                news_item.ai_summary = row['ai_summary']
                news_item.ai_topic = row['ai_topic']
                if news_item.status != 'fully_enriched':
                    news_item.status = 'content_enriched'
                updated_count += 1
            
            if (index + 1) % 10 == 0 or index == total_items - 1:
                logging.info(f"Updated {index + 1}/{total_items} items")
        
        session.commit()
        logging.info(f"Successfully updated {updated_count} news items with enriched content")
    except Exception as e:
        logging.error(f"Error updating enriched news: {str(e)}")
        session.rollback()
    finally:
        session.close()

def process_publisher(publisher):
    logging.info(f"Processing publisher: {publisher}")
    
    news_without_content = get_news_without_content(publisher)
    if not news_without_content:
        logging.info(f"No news items without content found for {publisher}. Skipping.")
        return
    
    news_df = news_to_dataframe(news_without_content)
    
    logging.info(f"Enriching news items with content for {publisher}")
    enriched_df = enrich_content_from_url(news_df)
    
    update_enriched_news(enriched_df)

def main():
    start_time = time.time()
    logging.info("Starting content enrichment task")
    
    for publisher in PUBLISHERS:
        process_publisher(publisher)
    
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Content enrichment task completed for all publishers. Duration: {duration:.2f} seconds")

if __name__ == "__main__":
    main()
