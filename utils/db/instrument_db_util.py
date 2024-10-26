from sqlalchemy import Column, Integer, String, Text, Float, select, delete, func, text
from sqlalchemy.orm import sessionmaker
from utils.db.news_db_util import Base, engine
from utils.logging.log_util import get_logger
import pandas as pd
import re

logger = get_logger(__name__)

Session = sessionmaker(bind=engine)

class Instrument(Base):
    __tablename__ = 'instrument'

    id = Column(Integer, primary_key=True)
    issuer = Column(String(255))
    ticker = Column(String(100))
    yf_ticker = Column(String(100))
    isin = Column(String(100))
    asset_class = Column(String(100))
    sector = Column(String(100))
    exchange = Column(String(100))
    exchange_code = Column(String(10))
    country = Column(String(10))
    url = Column(Text)

def save_instrument(df):
    session = Session()
    try:
        logger.info(f"Starting to process {len(df)} instruments")
        
        # Filter out rows where yf_ticker is None or empty string
        df_valid = df[df['yf_ticker'].notna() & (df['yf_ticker'] != '')]
        
        # Check for existing yf_tickers in the database
        existing_instruments = session.query(Instrument).filter(
            Instrument.yf_ticker.in_(df_valid['yf_ticker'].tolist())
        ).all()
        existing_yf_tickers = {instrument.yf_ticker: instrument for instrument in existing_instruments}
        
        new_instruments = 0
        updated_instruments = 0
        
        for _, row in df_valid.iterrows():
            yf_ticker = str(row['yf_ticker'])
            if yf_ticker in existing_yf_tickers:
                # Update existing instrument
                instrument = existing_yf_tickers[yf_ticker]
                updated_instruments += 1
            else:
                # Create new instrument
                instrument = Instrument()
                new_instruments += 1
            
            # Update all fields
            instrument.issuer = None if pd.isna(row['issuer']) else str(row['issuer'])
            instrument.ticker = None if pd.isna(row['ticker']) else str(row['ticker'])
            instrument.yf_ticker = yf_ticker
            instrument.isin = None if pd.isna(row['isin']) else str(row['isin'])
            instrument.asset_class = None if pd.isna(row['asset_class']) else str(row['asset_class'])
            instrument.exchange = None if pd.isna(row['exchange']) else str(row['exchange'])
            instrument.exchange_code = None if pd.isna(row['exchange_code']) else str(row['exchange_code'])
            instrument.country = None if pd.isna(row['country']) else str(row['country'])
            instrument.url = None if pd.isna(row['url']) else str(row['url'])
            
            session.add(instrument)
        
        session.commit()
        logger.info(f"Processed {len(df_valid)} instruments. New: {new_instruments}, Updated: {updated_instruments}")
    except Exception as e:
        session.rollback()
        logger.error(f"An error occurred while saving instruments: {e}")
    finally:
        session.close()

def get_instrument_by_ticker(ticker):
    session = Session()
    try:
        logger.info(f"Fetching instrument by ticker: {ticker}")
        return session.query(Instrument).filter_by(ticker=ticker).first()
    finally:
        session.close()

def get_instrument_by_company_name(company_name):
    session = Session()
    try:
        logger.info(f"Fetching instrument by company name: {company_name}")        
        # Simple where condition on company name and issuer
        query = select(Instrument).where(Instrument.issuer == company_name)
        
        return session.execute(query).scalars().first()
    finally:
        session.close()

def get_all_instruments():
    session = Session()
    try:
        logger.info("Fetching all instruments")
        query = select(Instrument)
        result = session.execute(query)
        instruments = result.scalars().all()
        return pd.DataFrame([vars(instrument) for instrument in instruments])
    finally:
        session.close()

def delete_instruments(ids):
    session = Session()
    try:
        logger.info(f"Deleting instruments with IDs: {ids}")
        delete_stmt = delete(Instrument).where(Instrument.id.in_(ids))
        session.execute(delete_stmt)
        session.commit()
        logger.info(f"Successfully deleted {len(ids)} instruments")
    except Exception as e:
        session.rollback()
        logger.error(f"An error occurred while deleting instruments: {e}")
    finally:
        session.close()

# Add this at the end of the file to recreate the table if needed
if __name__ == "__main__":
    logger.info("Recreating Instrument table")
    Base.metadata.drop_all(engine, tables=[Instrument.__table__])
    Base.metadata.create_all(engine, tables=[Instrument.__table__])
    logger.info("Instrument table recreated successfully")
