import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import UUID
import uuid
import pandas as pd
from typing import Dict
import logging

# Load environment variables
load_dotenv()

# Get DATABASE_URL from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')

# Create SQLAlchemy engine and session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

class ModelResultsBinary(Base):
    __tablename__ = 'eq_model_results_binary'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(UUID(as_uuid=True), default=uuid.uuid4, nullable=False)
    timestamp = Column(DateTime, default=func.now(), nullable=False)
    event = Column(String(255), nullable=False)
    accuracy = Column(Float, nullable=False)
    precision = Column(Float)
    recall = Column(Float)
    f1_score = Column(Float)
    auc_roc = Column(Float)
    test_sample = Column(Integer, nullable=False)
    training_sample = Column(Integer, nullable=False)
    total_sample = Column(Integer, nullable=False)

class ModelResultsRegression(Base):
    __tablename__ = 'eq_model_results_regression'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(UUID(as_uuid=True), default=uuid.uuid4, nullable=False)
    timestamp = Column(DateTime, default=func.now(), nullable=False)
    event = Column(String(255), nullable=False)
    mse = Column(Float, nullable=False)
    r2 = Column(Float, nullable=False)
    mae = Column(Float, nullable=False)
    rmse = Column(Float, nullable=False)
    test_sample = Column(Integer, nullable=False)
    training_sample = Column(Integer, nullable=False)
    total_sample = Column(Integer, nullable=False)

def create_tables():
    Base.metadata.create_all(engine)

def save_results(results_df):
    session = Session()
    run_id = uuid.uuid4()
    try:
        for _, row in results_df.iterrows():
            result = ModelResultsBinary(
                run_id=run_id,
                event=row['event'],
                accuracy=row['accuracy'],
                precision=row['precision'],
                recall=row['recall'],
                f1_score=row['f1_score'],
                auc_roc=row['auc_roc'],
                test_sample=row['test_sample'],
                training_sample=row['training_sample'],
                total_sample=row['total_sample']
            )
            session.add(result)
        session.commit()
        logging.info(f'Successfully saved results to database with run_id: {run_id}')
        return True, run_id
    except Exception as e:
        logging.error(f'An error occurred while saving model results: {str(e)}')
        session.rollback()
        return False, None
    finally:
        session.close()

def save_regression_results(results_df):
    session = Session()
    run_id = uuid.uuid4()
    try:
        for _, row in results_df.iterrows():
            result = ModelResultsRegression(
                run_id=run_id,
                event=row['event'],
                mse=row['mse'],
                r2=row['r2'],
                mae=row['mae'],
                rmse=row['rmse'],
                test_sample=row['test_sample'],
                training_sample=row['training_sample'],
                total_sample=row['total_sample']
            )
            session.add(result)
        session.commit()
        logging.info(f'Successfully saved regression results to database with run_id: {run_id}')
        return True, run_id
    except Exception as e:
        logging.error(f'An error occurred while saving regression model results: {str(e)}')
        session.rollback()
        return False, None
    finally:
        session.close()

def get_results(run_id: str = None) -> pd.DataFrame:
    session = Session()
    try:
        query = session.query(ModelResultsBinary)
        if run_id:
            query = query.filter(ModelResultsBinary.run_id == uuid.UUID(run_id))
        results = query.all()
        data = [{
            'run_id': str(result.run_id),
            'timestamp': result.timestamp,
            'event': result.event,
            'accuracy': result.accuracy,
            'precision': result.precision,
            'recall': result.recall,
            'f1_score': result.f1_score,
            'auc_roc': result.auc_roc,
            'test_sample': result.test_sample,
            'training_sample': result.training_sample,
            'total_sample': result.total_sample
        } for result in results]
        return pd.DataFrame(data)
    finally:
        session.close()

def get_regression_results(run_id: str = None) -> pd.DataFrame:
    session = Session()
    try:
        query = session.query(ModelResultsRegression)
        if run_id:
            query = query.filter(ModelResultsRegression.run_id == uuid.UUID(run_id))
        results = query.all()
        data = [{
            'run_id': str(result.run_id),
            'timestamp': result.timestamp,
            'event': result.event,
            'mse': result.mse,
            'r2': result.r2,
            'mae': result.mae,
            'rmse': result.rmse,
            'test_sample': result.test_sample,
            'training_sample': result.training_sample,
            'total_sample': result.total_sample
        } for result in results]
        return pd.DataFrame(data)
    finally:
        session.close()

def get_accuracy(event: str) -> float:
    session = Session()
    try:
        # Convert event to lowercase and replace spaces with underscores
        #formatted_event = event.lower().replace(' ', '_')
        result = session.query(ModelResultsBinary.accuracy).filter(ModelResultsBinary.event == event).first()
        return result[0] if result else None
    except Exception as e:
        logging.error(f'An error occurred while fetching accuracy for event {event}: {str(e)}')
        return None
    finally:
        session.close()