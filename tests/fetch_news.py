import os
import feedparser
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.openai_utils import summarize, tag_news

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
    title = Column(String)
    link = Column(String)
    description = Column(Text)
    published_date = Column(DateTime)
    summary = Column(Text)
    ai_insights = Column(Text)

def fetch_news(url):
    feed = feedparser.parse(url)
    news_items = []

    for entry in feed.entries:
        summary = summarize(entry.description)
        ai_insights = summarize(f"Provide insights on this news: {entry.title}\n{entry.description}")
        
        news_item = News(
            title=entry.title,
            link=entry.link,
            description=entry.description,
            published_date=datetime.strptime(entry.published, '%a, %d %b %Y %H:%M:%S %z'),
            summary=summary,
            ai_insights=ai_insights
        )
        news_items.append(news_item)

    return news_items

def main():
    # Create tables
    Base.metadata.create_all(engine)

    # RSS feed URL (replace with your desired news feed)
    rss_url = 'http://rss.cnn.com/rss/cnn_topstories.rss'

    # Fetch news
    news_items = fetch_news(rss_url)

    # Store news in the database
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

if __name__ == '__main__':
    main()