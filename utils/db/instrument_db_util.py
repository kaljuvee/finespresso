from sqlalchemy import Column, Integer, String, Text, Float
from sqlalchemy.orm import sessionmaker
from utils.db.news_db_util import Base, engine
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

Session = sessionmaker(bind=engine)

class Instrument(Base):
    __tablename__ = 'instrument'

    id = Column(Integer, primary_key=True)
    issuer = Column(String(255))
    ticker = Column(String(100))
    yf_ticker = Column(String(100))
    isin = Column(String(100))
    asset_class = Column(String(100))
    exchange = Column(String(100))
    exchange_code = Column(String(10))
    country = Column(String(10))
    url = Column(Text)

def save_instrument(df):
    session = Session()
    try:
        logging.info(f"Starting to save {len(df)} instruments")
        for _, row in df.iterrows():
            instrument = Instrument(
                issuer=None if pd.isna(row['issuer']) else str(row['issuer']),
                ticker=None if pd.isna(row['ticker']) else str(row['ticker']),
                yf_ticker=None if pd.isna(row['yf_ticker']) else str(row['yf_ticker']),
                isin=None if pd.isna(row['isin']) else str(row['isin']),
                asset_class=None if pd.isna(row['asset_class']) else str(row['asset_class']),
                exchange=None if pd.isna(row['exchange']) else str(row['exchange']),
                exchange_code=None if pd.isna(row['exchange_code']) else str(row['exchange_code']),
                country=None if pd.isna(row['country']) else str(row['country']),
                url=None if pd.isna(row['url']) else str(row['url'])
            )
            session.add(instrument)
        session.commit()
        logging.info("All instruments saved successfully")
    except Exception as e:
        session.rollback()
        logging.error(f"An error occurred while saving instruments: {e}")
    finally:
        session.close()

def get_instrument_by_ticker(ticker):
    session = Session()
    try:
        logging.info(f"Fetching instrument by ticker: {ticker}")
        return session.query(Instrument).filter_by(ticker=ticker).first()
    finally:
        session.close()

def get_instrument_by_company_name(company_name):
    session = Session()
    try:
        logging.info(f"Fetching instrument by company name: {company_name}")
        return session.query(Instrument).filter(Instrument.issuer.ilike(f"%{company_name}%")).first()
    finally:
        session.close()

# Add other methods as needed

# Add this at the end of the file to recreate the table if needed
if __name__ == "__main__":
    logging.info("Recreating Instrument table")
    Base.metadata.drop_all(engine, tables=[Instrument.__table__])
    Base.metadata.create_all(engine, tables=[Instrument.__table__])
    logging.info("Instrument table recreated successfully")
