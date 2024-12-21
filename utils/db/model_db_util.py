import os
from dotenv import load_dotenv
from sqlalchemy import Column, Integer, String, Float, DateTime, func
from utils.db.db_pool import DatabasePool
from sqlalchemy.dialects.postgresql import UUID
import uuid
import pandas as pd
from typing import Dict
import logging

# Load environment variables
load_dotenv()

# Get DATABASE_URL from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')

# Get database pool instance
db_pool = DatabasePool()

class ModelResultsBinary(db_pool.Base):
    __tablename__ = 'eq_model_results_binary'

    id = Column(Integer, primary_key=True, autoincrement=True)
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

class ModelResultsRegression(db_pool.Base):
    __tablename__ = 'eq_model_results_regression'

    id = Column(Integer, primary_key=True, autoincrement=True)
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
    db_pool.create_all_tables()

def save_results(results_df):
    with db_pool.get_session() as session:
        try:
            for _, row in results_df.iterrows():
                result = ModelResultsBinary(
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
            logging.info(f'Successfully saved results to database')
            return True
        except Exception as e:
            logging.error(f'An error occurred while saving model results: {str(e)}')
            return False

def save_regression_results(results_df):
    with db_pool.get_session() as session:
        try:
            for _, row in results_df.iterrows():
                result = ModelResultsRegression(
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
            logging.info(f'Successfully saved regression results to database')
            return True
        except Exception as e:
            logging.error(f'An error occurred while saving regression model results: {str(e)}')
            return False

def get_results(timestamp: str = None) -> pd.DataFrame:
    with db_pool.get_session() as session:
        query = session.query(ModelResultsBinary)
        if timestamp:
            query = query.filter(ModelResultsBinary.timestamp == timestamp)
        results = query.all()
        data = [{
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

def get_regression_results(timestamp: str = None) -> pd.DataFrame:
    with db_pool.get_session() as session:
        query = session.query(ModelResultsRegression)
        if timestamp:
            query = query.filter(ModelResultsRegression.timestamp == timestamp)
        results = query.all()
        data = [{
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

def get_accuracy(event: str) -> float:
    with db_pool.get_session() as session:
        try:
            result = session.query(ModelResultsBinary.accuracy).filter(ModelResultsBinary.event == event).first()
            return result[0] if result else None
        except Exception as e:
            logging.error(f'An error occurred while fetching accuracy for event {event}: {str(e)}')
            return None
