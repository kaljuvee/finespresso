import logging
from sqlalchemy import select
from utils.db_util import Session, News, engine
from utils.enrich_util import enrich_content_from_url
import pandas as pd
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_news_without_content():
    logging.info("Retrieving news items without content from database")
    session = Session()
    try:
        query = select(News).where(News.content.is_(None))
        result = session.execute(query)
        news_items = result.scalars().all()
        logging.info(f"Retrieved {len(news_items)} news items without content")
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
        for _, row in enriched_df.iterrows():
            news_item = session.query(News).get(row['id'])
            if news_item:
                news_item.content = row['content']
                news_item.ai_summary = row['ai_summary']
                news_item.ai_topic = row['ai_topic']
                if news_item.status != 'fully_enriched':
                    news_item.status = 'content_enriched'
                updated_count += 1
        session.commit()
        logging.info(f"Updated {updated_count} news items with enriched content")
    except Exception as e:
        logging.error(f"Error updating enriched news: {e}")
        session.rollback()
    finally:
        session.close()

def main():
    start_time = time.time()
    logging.info("Starting content enrichment task")
    
    news_without_content = get_news_without_content()
    if not news_without_content:
        logging.info("No news items without content found. Task completed.")
        return
    
    news_df = news_to_dataframe(news_without_content)
    enriched_df = enrich_content_from_url(news_df)
    update_enriched_news(enriched_df)
    
    end_time = time.time()
    logging.info(f"Content enrichment task completed. Duration: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
