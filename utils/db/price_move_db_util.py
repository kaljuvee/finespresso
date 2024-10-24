import logging
from datetime import datetime, time
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text, select, join
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
from sqlalchemy.exc import IntegrityError
import pandas as pd
from utils.db.news_db_util import News

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get DATABASE_URL from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class PriceMove(Base):
    __tablename__ = 'price_moves'

    id = Column(Integer, primary_key=True)
    news_id = Column(Integer, nullable=False)
    ticker = Column(String, nullable=False)
    published_date = Column(DateTime, nullable=False)
    begin_price = Column(Float, nullable=False)
    end_price = Column(Float, nullable=False)
    index_begin_price = Column(Float, nullable=False)
    index_end_price = Column(Float, nullable=False)
    volume = Column(Integer)
    market = Column(String, nullable=False)  # This line should be present
    price_change = Column(Float, nullable=False)
    price_change_percentage = Column(Float, nullable=False)
    index_price_change = Column(Float, nullable=False)
    index_price_change_percentage = Column(Float, nullable=False)
    daily_alpha = Column(Float, nullable=False)
    actual_side = Column(String(10), nullable=False)
    predicted_side = Column(String(10))
    predicted_move = Column(Float)

    def __init__(self, news_id, ticker, published_date, begin_price, end_price, index_begin_price, index_end_price,
                 volume, market, price_change, price_change_percentage, index_price_change, index_price_change_percentage,
                 daily_alpha, actual_side, predicted_side=None, predicted_move=None):
        self.news_id = str(news_id)  # Convert news_id to string
        self.ticker = ticker
        self.published_date = published_date
        self.begin_price = begin_price
        self.end_price = end_price
        self.index_begin_price = index_begin_price
        self.index_end_price = index_end_price
        self.volume = volume
        self.market = market
        self.price_change = price_change
        self.price_change_percentage = price_change_percentage
        self.index_price_change = index_price_change
        self.index_price_change_percentage = index_price_change_percentage
        self.daily_alpha = daily_alpha
        self.actual_side = actual_side
        self.predicted_side = predicted_side
        self.predicted_move = predicted_move

def store_price_move(price_move):
    try:
        session = Session()
        
        # Convert news_id to string when querying
        existing_price_move = session.query(PriceMove).filter_by(news_id=str(price_move.news_id)).first()
        
        if existing_price_move:
            # Update existing record
            for key, value in price_move.__dict__.items():
                if key != '_sa_instance_state':  # Skip SQLAlchemy internal attribute
                    setattr(existing_price_move, key, value)
            logger.info(f"Updated existing price move for news_id: {price_move.news_id}, ticker: {price_move.ticker}")
        else:
            # Add new record
            session.add(price_move)
            logger.info(f"Added new price move for news_id: {price_move.news_id}, ticker: {price_move.ticker}")
        
        session.commit()
    except IntegrityError as ie:
        logger.error(f"IntegrityError storing price move for news_id {price_move.news_id}: {str(ie)}")
        session.rollback()
    except Exception as e:
        logger.error(f"Error storing price move for news_id {price_move.news_id}: {str(e)}")
        logger.exception("Detailed traceback:")
        session.rollback()
    finally:
        session.close()

    # Verify that the price move was stored or updated
    try:
        verify_session = Session()
        # Convert news_id to string when querying
        stored_price_move = verify_session.query(PriceMove).filter_by(news_id=str(price_move.news_id)).first()
        if stored_price_move:
            logger.info(f"Verified: Price move for news_id {price_move.news_id} is in the database")
        else:
            logger.warning(f"Verification failed: Price move for news_id {price_move.news_id} not found in the database")
    except Exception as e:
        logger.error(f"Error verifying price move storage: {str(e)}")
    finally:
        verify_session.close()

def get_news_price_moves():
    try:
        session = Session()
        
        query = select(News.id, PriceMove.price_change_percentage).select_from(
            join(News, PriceMove, News.id == PriceMove.news_id)
        )
        
        result = session.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=['id', 'price_change_percentage'])
        
        logging.info(f"Retrieved {len(df)} rows from news and price_moves tables")
        return df
    except Exception as e:
        logging.error(f"Error retrieving news and price moves: {str(e)}")
        return pd.DataFrame()
    finally:
        session.close()

# Create tables
Base.metadata.create_all(engine)

def add_market_column():
    engine = create_engine(DATABASE_URL)
    with engine.connect() as connection:
        connection.execute(text("ALTER TABLE price_moves ADD COLUMN market VARCHAR(255) NOT NULL DEFAULT 'market_open';"))
        connection.commit()

# Call this function once to add the column
# add_market_column()
