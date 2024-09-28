import logging
from sqlalchemy import select
from utils.db_util import Session, News
from utils.enrich_util import enrich_tag_from_url
import pandas as pd
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_news_without_tags():
    print("Retrieving news items without AI topics...")
    logging.info("Retrieving news items without AI topics from database")
    session = Session()
    try:
        query = select(News).where(News.ai_topic.is_(None))
        result = session.execute(query)
        news_items = result.scalars().all()
        count = len(news_items)
        print(f"Retrieved {count} news items without AI topics")
        logging.info(f"Retrieved {count} news items without AI topics")
        return news_items
    finally:
        session.close()

def news_to_dataframe(news_items):
    print("Converting news items to DataFrame...")
    logging.info("Converting news items to DataFrame")
    df = pd.DataFrame([{'id': item.id, 'link': item.link} for item in news_items])
    print(f"Created DataFrame with {len(df)} rows")
    logging.info(f"Created DataFrame with {len(df)} rows")
    return df

def update_tags(enriched_df):
    print("Updating database with enriched tags...")
    logging.info("Updating database with enriched tags")
    session = Session()
    try:
        updated_count = 0
        total_items = len(enriched_df)
        for index, row in enriched_df.iterrows():
            news_item = session.get(News, row['id'])
            if news_item and 'ai_topic' in row:
                news_item.ai_topic = row['ai_topic']
                updated_count += 1
            
            if (index + 1) % 10 == 0 or index == total_items - 1:
                print(f"Updated {index + 1}/{total_items} items")
                logging.info(f"Updated {index + 1}/{total_items} items")
        
        session.commit()
        print(f"Successfully updated {updated_count} news items with tags")
        logging.info(f"Successfully updated {updated_count} news items with tags")
    except Exception as e:
        print(f"Error updating tags: {str(e)}")
        logging.error(f"Error updating tags: {str(e)}")
        session.rollback()
    finally:
        session.close()

def main():
    start_time = time.time()
    print("Starting tag enrichment task")
    logging.info("Starting tag enrichment task")
    
    news_without_tags = get_news_without_tags()
    if not news_without_tags:
        print("No news items without AI topics found. Task completed.")
        logging.info("No news items without AI topics found. Task completed.")
        return
    
    news_df = news_to_dataframe(news_without_tags)
    
    print("Enriching news items with tags...")
    logging.info("Enriching news items with tags")
    enriched_df = enrich_tag_from_url(news_df)
    
    update_tags(enriched_df)
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"Tag enrichment task completed. Duration: {duration:.2f} seconds")
    logging.info(f"Tag enrichment task completed. Duration: {duration:.2f} seconds")

if __name__ == "__main__":
    main()
