import logging
from sqlalchemy import func, and_
from utils.news_db_util import Session, News

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def remove_duplicate_news():
    session = Session()
    try:
        # Step 1: Remove duplicates
        # Subquery to find the oldest record for each link among 'raw' status items
        subquery = session.query(News.link, func.min(News.downloaded_at).label('min_downloaded_at')) \
                          .filter(News.status == 'raw') \
                          .group_by(News.link) \
                          .subquery()
        
        # Query to select duplicate records that are not the oldest among 'raw' status items
        duplicates = session.query(News.id) \
                            .filter(News.status == 'raw') \
                            .join(subquery, and_(News.link == subquery.c.link,
                                                 News.downloaded_at != subquery.c.min_downloaded_at))
        
        # Delete the duplicates
        deleted_count = session.query(News).filter(News.id.in_(duplicates)).delete(synchronize_session='fetch')
        
        # Step 2: Update status of remaining 'raw' items to 'clean'
        updated_count = session.query(News).filter(News.status == 'raw') \
                               .update({News.status: 'clean'}, synchronize_session='fetch')
        
        session.commit()
        logging.info(f"Successfully removed {deleted_count} duplicate news items with 'raw' status.")
        logging.info(f"Updated status from 'raw' to 'clean' for {updated_count} news items.")
        return deleted_count, updated_count
    except Exception as e:
        logging.error(f"An error occurred while removing duplicates and updating status: {e}")
        session.rollback()
        return 0, 0
    finally:
        session.close()

def main():
    logging.basicConfig(level=logging.INFO)
    deleted_count, updated_count = remove_duplicate_news()
    logging.info(f"Total deleted: {deleted_count}, Total updated: {updated_count}")

if __name__ == "__main__":
    main()
