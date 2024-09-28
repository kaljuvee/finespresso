import logging
from sqlalchemy import select
from utils.db_util import Session, News
from utils.enrich_util import enrich_from_url
import pandas as pd
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_news_without_summary():
    logging.info("Retrieving news items without summaries from database")
    session = Session()
    try:
        query = select(News).where(News.ai_summary.is_(None))
        result = session.execute(query)
        news_items = result.scalars().all()
        logging.info(f"Retrieved {len(news_items)} news items without summaries")
        return news_items
    finally:
        session.close()

def news_to_dataframe(news_items):
    return pd.DataFrame([{'id': item.id, 'link': item.link} for item in news_items])

def update_summaries(enriched_df):
    session = Session()
    try:
        updated_count = 0
        for _, row in enriched_df.iterrows():
            news_item = session.get(News, row['id'])
            if news_item and 'ai_summary' in row:
                news_item.ai_summary = row['ai_summary']
                updated_count += 1
        session.commit()
        logging.info(f"Updated {updated_count} news items with summaries")
    except Exception as e:
        logging.error(f"Error updating summaries: {str(e)}")
        session.rollback()
    finally:
        session.close()

def main():
    start_time = time.time()
    logging.info("Starting summary enrichment task")
    
    news_without_summary = get_news_without_summary()
    if not news_without_summary:
        logging.info("No news items without summaries found. Task completed.")
        return
    
    news_df = news_to_dataframe(news_without_summary)
    enriched_df = enrich_from_url(news_df)
    update_summaries(enriched_df)
    
    end_time = time.time()
    logging.info(f"Summary enrichment task completed. Duration: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
