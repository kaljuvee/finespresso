from sqlalchemy import Column, Integer, String, Text, select, delete, update, func, or_
from sqlalchemy.orm import sessionmaker
from utils.db.news_db_util import Base, engine
from utils.logging.log_util import get_logger
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import re

logger = get_logger(__name__)

Session = sessionmaker(bind=engine)

class Instrument(Base):
    __tablename__ = 'instrument'

    id = Column(Integer, primary_key=True)
    issuer = Column(String(255))
    ticker = Column(String(100))
    yf_ticker = Column(String(100), unique=True, index=True)
    isin = Column(String(100))
    asset_class = Column(String(100))
    sector = Column(String(100))
    exchange = Column(String(100))
    exchange_code = Column(String(10))
    country = Column(String(10))
    url = Column(Text)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

def save_instrument(instrument_data):
    session = Session()
    try:
        if 'id' in instrument_data and instrument_data['id']:
            # Update existing instrument
            instrument = session.query(Instrument).get(instrument_data['id'])
            if instrument:
                for key, value in instrument_data.items():
                    if hasattr(instrument, key) and key != 'id':
                        setattr(instrument, key, value)
                logger.info(f"Updated instrument with ID: {instrument.id}")
            else:
                logger.warning(f"Instrument with ID {instrument_data['id']} not found")
                return None
        else:
            # Create new instrument
            instrument = Instrument(**instrument_data)
            session.add(instrument)
            logger.info("Created new instrument")

        session.commit()
        return instrument.to_dict()
    except SQLAlchemyError as e:
        session.rollback()
        logger.exception(f"A database error occurred while saving instrument: {e}")
        raise Exception(f"A database error occurred: {str(e)}")
    except Exception as e:
        session.rollback()
        logger.exception(f"An unexpected error occurred while saving instrument: {e}")
        raise
    finally:
        session.close()

def get_instrument_by_ticker(ticker):
    session = Session()
    try:
        logger.info(f"Fetching instrument by ticker: {ticker}")
        return session.query(Instrument).filter_by(ticker=ticker).first()
    finally:
        session.close()

# Replace the existing get_instrument_by_company_name function with this updated version

def format_search_term(term):
    """
    Lowercase the term and remove special characters.
    """
    return re.sub(r'[^a-zA-Z0-9]', '', term.lower())

def get_instrument_by_company_name(company_name):
    logger.info(f"Looking up instrument for company: {company_name}")
    
    session = Session()
    try:
        formatted_company_name = format_search_term(company_name)
        instruments = session.query(Instrument).filter(
            or_(
                func.lower(func.regexp_replace(Instrument.issuer, '[^a-zA-Z0-9]', '', 'g')) == formatted_company_name,
                func.lower(func.regexp_replace(Instrument.issuer, '[^a-zA-Z0-9]', '', 'g')).like(f"%{formatted_company_name}%")
            )
        ).all()
        
        if instruments:
            instrument = instruments[0]
            logger.info(f"Found instrument for {company_name}: {instrument.ticker}")
            if len(instruments) > 1:
                logger.warning(f"Multiple instruments found for {company_name}. Using the first match: {instrument.ticker}")
        else:
            instrument = None
            logger.info(f"No instrument found for {company_name}")
        
        return instrument
    except SQLAlchemyError as e:
        logger.error(f"A database error occurred while querying for {company_name}: {str(e)}")
        return None
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

def insert_instrument(instrument_data):
    session = Session()
    try:
        # Check if yf_ticker already exists
        existing_instrument = session.query(Instrument).filter_by(yf_ticker=instrument_data['yf_ticker']).first()
        if existing_instrument:
            return None, f"Instrument with yf_ticker '{instrument_data['yf_ticker']}' already exists."

        # Create new instrument
        instrument = Instrument(**instrument_data)
        session.add(instrument)
        session.commit()
        logger.info(f"Inserted new instrument with ID: {instrument.id}")
        return instrument.to_dict(), "Instrument inserted successfully."
    except IntegrityError as e:
        session.rollback()
        logger.exception(f"An integrity error occurred while inserting instrument: {e}")
        return None, f"An integrity error occurred: {str(e)}"
    except SQLAlchemyError as e:
        session.rollback()
        logger.exception(f"A database error occurred while inserting instrument: {e}")
        return None, f"A database error occurred: {str(e)}"
    except Exception as e:
        session.rollback()
        logger.exception(f"An unexpected error occurred while inserting instrument: {e}")
        return None, f"An unexpected error occurred: {str(e)}"
    finally:
        session.close()

# Add this at the end of the file to recreate the table if needed
if __name__ == "__main__":
    logger.info("Recreating Instrument table")
    Base.metadata.drop_all(engine, tables=[Instrument.__table__])
    Base.metadata.create_all(engine, tables=[Instrument.__table__])
    logger.info("Instrument table recreated successfully")

# Add this new function after the existing functions

def get_instrument_by_yf_ticker(yf_ticker):
    session = Session()
    try:
        logger.info(f"Fetching instrument by Yahoo Finance ticker: {yf_ticker}")
        return session.query(Instrument).filter_by(yf_ticker=yf_ticker).first()
    finally:
        session.close()

# Add this new function at the end of the file

def get_distinct_instrument_fields():
    session = Session()
    try:
        logger.info("Fetching distinct instrument fields")
        asset_classes = session.query(Instrument.asset_class).distinct().order_by(Instrument.asset_class).all()
        sectors = session.query(Instrument.sector).distinct().order_by(Instrument.sector).all()
        exchanges = session.query(Instrument.exchange).distinct().order_by(Instrument.exchange).all()
        exchange_codes = session.query(Instrument.exchange_code).distinct().order_by(Instrument.exchange_code).all()
        countries = session.query(Instrument.country).distinct().order_by(Instrument.country).all()

        return {
            'asset_classes': [ac[0] for ac in asset_classes if ac[0]],
            'sectors': [s[0] for s in sectors if s[0]],
            'exchanges': [e[0] for e in exchanges if e[0]],
            'exchange_codes': [ec[0] for ec in exchange_codes if ec[0]],
            'countries': [c[0] for c in countries if c[0]]
        }
    finally:
        session.close()
