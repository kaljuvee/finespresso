from sqlalchemy import Column, Integer, String, Float, DateTime, text, select, join
from utils.db.db_pool import DatabasePool
from datetime import datetime, time
from utils.logging.log_util import get_logger
import pandas as pd
from utils.db.news_db_util import News

logger = get_logger(__name__)

# Get database pool instance
db_pool = DatabasePool()

class PriceMove(db_pool.Base):
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
        with db_pool.get_session() as session:
            existing_price_move = session.query(PriceMove).filter_by(news_id=str(price_move.news_id)).first()
            
            if existing_price_move:
                for key, value in price_move.__dict__.items():
                    if key != '_sa_instance_state':
                        setattr(existing_price_move, key, value)
                logger.info(f"Updated existing price move for news_id: {price_move.news_id}, ticker: {price_move.ticker}")
            else:
                session.add(price_move)
                logger.info(f"Added new price move for news_id: {price_move.news_id}, ticker: {price_move.ticker}")

    except Exception as e:
        logger.error(f"Error storing price move for news_id {price_move.news_id}: {str(e)}")
        logger.exception("Detailed traceback:")
        raise

    # Verify that the price move was stored or updated
    try:
        with db_pool.get_session() as session:
            stored_price_move = session.query(PriceMove).filter_by(news_id=str(price_move.news_id)).first()
            if stored_price_move:
                logger.info(f"Verified: Price move for news_id {price_move.news_id} is in the database")
            else:
                logger.warning(f"Verification failed: Price move for news_id {price_move.news_id} not found in the database")
    except Exception as e:
        logger.error(f"Error verifying price move storage: {str(e)}")

def get_news_price_moves():
    try:
        with db_pool.get_session() as session:
            query = select(News.id, News.content, News.title, News.event, 
                           PriceMove.price_change_percentage, PriceMove.daily_alpha, 
                           PriceMove.actual_side).select_from(
                join(News, PriceMove, News.id == PriceMove.news_id)
            )
            
            result = session.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=['id', 'content', 'title', 'event', 
                                                          'price_change_percentage', 'daily_alpha', 
                                                          'actual_side'])
            
            logger.info(f"Retrieved {len(df)} rows from news and price_moves tables")
            return df
    except Exception as e:
        logger.error(f"Error retrieving news and price moves: {str(e)}")
        return pd.DataFrame()

# Create tables
db_pool.create_all_tables()

def add_market_column():
    with db_pool.get_session() as session:
        connection = session.connection()
        connection.execute(text("ALTER TABLE price_moves ADD COLUMN market VARCHAR(255) NOT NULL DEFAULT 'market_open';"))

# Call this function once to add the column
# add_market_column()
