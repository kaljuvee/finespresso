import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, func, and_, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import TIMESTAMP
import logging
from datetime import datetime
import pandas as pd
import streamlit as st

# Load environment variables
load_dotenv()

# Get DATABASE_URL from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')

# Create SQLAlchemy engine and session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
# Define the News model
Base = declarative_base()

class News(Base):
    __tablename__ = 'news'

    id = Column(Integer, primary_key=True)
    title = Column(String(255))
    link = Column(String(255))
    company = Column(String(255))
    published_date = Column(TIMESTAMP(timezone=True))
    content = Column(Text)
    ai_summary = Column(Text)
    ai_topic = Column(String(255))
    industry = Column(String(255))
    publisher_topic = Column(String(255))
    publisher = Column(String(255))
    downloaded_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)
    status = Column(String(255))

class Signups(Base):
    __tablename__ = 'signups'

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False)
    captured_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow)

def create_tables():
    Base.metadata.create_all(engine)

def add_news_items(news_items):
    session = Session()
    try:
        for item in news_items:
            item.downloaded_at = datetime.utcnow()
        session.add_all(news_items)
        session.commit()
        print(f"Successfully added {len(news_items)} news items to the database.")
    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()
    finally:
        session.close()

def map_to_db(df, source):
    logging.info(f"Mapping dataframe to News objects for source: {source}")
    news_items = []
    for _, row in df.iterrows():
        news_item = News(
            title=row['title'],
            link=row['link'],
            company=row['company'],
            published_date=row['published_date'],
            publisher_topic=row['publisher_topic'],
            publisher=row['publisher'],
            downloaded_at=datetime.utcnow(),
            status=row['status']
        )

        # Map industry only if source is 'euronext'
        if source == 'euronext':
            news_item.industry = row['industry']

        # Map AI-related fields only if publisher is 'ai'
        if row['publisher'] == 'ai':
            news_item.ai_summary = row['ai_summary']
            news_item.ai_topic = row['ai_topic']

        news_items.append(news_item)
    
    logging.info(f"Created {len(news_items)} News objects")
    return news_items

def remove_duplicate_news():
    session = Session()
    try:
        # Step 1: Remove duplicates
        # Subquery to find the oldest record for each link
        subquery = session.query(News.link, func.min(News.downloaded_at).label('min_downloaded_at')) \
                          .group_by(News.link) \
                          .subquery()
        
        # Query to select duplicate records that are not the oldest
        duplicates = session.query(News.id) \
                            .join(subquery, and_(News.link == subquery.c.link,
                                                 News.downloaded_at != subquery.c.min_downloaded_at))
        
        # Delete the duplicates
        deleted_count = session.query(News).filter(News.id.in_(duplicates)).delete(synchronize_session='fetch')
        
        # Step 2: Update status of remaining items
        updated_count = session.query(News).filter(News.status == 'raw').update({News.status: 'clean'}, synchronize_session='fetch')
        
        session.commit()
        logging.info(f"Successfully removed {deleted_count} duplicate news items.")
        logging.info(f"Updated status to 'clean' for {updated_count} news items.")
        return deleted_count, updated_count
    except Exception as e:
        logging.error(f"An error occurred while removing duplicates and updating status: {e}")
        session.rollback()
        return 0, 0
    finally:
        session.close()

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_news_df(publisher):
    session = Session()
    try:
        query = select(News).where(News.publisher == publisher).order_by(News.published_date.desc())
        result = session.execute(query)
        news_items = result.scalars().all()
        
        data = [{
            'Title': item.title,
            'Link': item.link,
            'Date': item.published_date,  # Changed back to 'Publication Date'
            'Company': item.company,
            'Event': item.ai_topic,
            'Why it Moves?': item.ai_summary
        } for item in news_items]
        
        return pd.DataFrame(data)
    finally:
        session.close()

def save_email(email):
    session = Session()
    try:
        new_signup = Signups(email=email)
        session.add(new_signup)
        session.commit()
        logging.info(f"Successfully added email: {email}")
        return True
    except Exception as e:
        logging.error(f"An error occurred while saving email: {e}")
        session.rollback()
        return False
    finally:
        session.close()