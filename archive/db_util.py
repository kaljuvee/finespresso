import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import TIMESTAMP
import logging

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
    ai_summary = Column(Text)
    industry = Column(String(255))
    publisher_topic = Column(String(255))
    ai_topic = Column(String(255))
    publisher = Column(String(255))

def create_tables():
    Base.metadata.create_all(engine)

def add_news_items(news_items):
    session = Session()
    try:
        session.add_all(news_items)
        session.commit()
        print(f"Successfully added {len(news_items)} news items to the database.")
    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()
    finally:
        session.close()

def map_to_db(df):
    logging.info("Mapping dataframe to News objects")
    news_items = []
    for _, row in df.iterrows():
        news_item = News(
            title=row['title'],
            link=row['link'],
            company=row['company'],
            published_date=row['published_date'],
            ai_summary=row['ai_summary'],
            industry=row['industry'],
            publisher_topic=row['publisher_topic'],
            ai_topic=row['ai_topic'],
            publisher=row['publisher']
        )
        news_items.append(news_item)
    logging.info(f"Created {len(news_items)} News objects")
    return news_items