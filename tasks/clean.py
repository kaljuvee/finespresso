import logging
from sqlalchemy import func, and_
from utils.db.news_db_util import Session, News

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def remove_duplicate_news():
    session = Session()
    try:
        # Subquery to find the oldest record ID for each link
        subquery = session.query(News.link, func.min(News.id).label('min_id')) \
                          .group_by(News.link) \
                          .subquery()
        
        # Query to select duplicate records that are not the oldest (by ID)
        duplicates = session.query(News.id) \
                            .join(subquery, and_(News.link == subquery.c.link,
                                                 News.id != subquery.c.min_id))
        
        # Delete the duplicates
        deleted_count = session.query(News).filter(News.id.in_(duplicates)).delete(synchronize_session='fetch')
        
        session.commit()
        logging.info(f"Successfully removed {deleted_count} duplicate news items.")
        return deleted_count
    except Exception as e:
        logging.error(f"An error occurred while removing duplicates: {e}")
        session.rollback()
        return 0
    finally:
        session.close()

def main():
    logging.basicConfig(level=logging.INFO)
    deleted_count = remove_duplicate_news()
    logging.info(f"Total deleted: {deleted_count}")

if __name__ == "__main__":
    main()
